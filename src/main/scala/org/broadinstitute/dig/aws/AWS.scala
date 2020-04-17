package org.broadinstitute.dig.aws

import java.io.InputStream
import java.net.URI

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.io.Source
import scala.util.Random

import org.broadinstitute.dig.aws.config.AWSConfig
import org.broadinstitute.dig.aws.emr.Cluster

import com.typesafe.scalalogging.LazyLogging

import cats.effect.ContextShift
import cats.effect.ExitCase
import cats.effect.IO
import cats.effect.Timer
import cats.implicits._

import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.emr.EmrClient
import software.amazon.awssdk.services.emr.model.AddJobFlowStepsRequest
import software.amazon.awssdk.services.emr.model.AddJobFlowStepsResponse
import software.amazon.awssdk.services.emr.model.JobFlowInstancesConfig
import software.amazon.awssdk.services.emr.model.ListStepsRequest
import software.amazon.awssdk.services.emr.model.RunJobFlowRequest
import software.amazon.awssdk.services.emr.model.RunJobFlowResponse
import software.amazon.awssdk.services.emr.model.StepSummary
import software.amazon.awssdk.services.emr.model.TerminateJobFlowsRequest
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.Delete
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetUrlRequest
import software.amazon.awssdk.services.s3.model.ObjectIdentifier
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import software.amazon.awssdk.core.ResponseInputStream

/** AWS controller (S3 + EMR clients).
  */
final class AWS(config: AWSConfig) extends LazyLogging {
  import Implicits._

  /** The same region and bucket are used for all operations.
    */
  val bucket: String = config.s3.bucket

  /** S3 client for storage.
    */
  val s3: S3Client = S3Client.builder.build

  /** EMR client for running map/reduce jobs.
    */
  val emr: EmrClient = EmrClient.builder.build

  /** Returns the URI to a given key.
    */
  def uriOf(key: String): URI = new URI(s"s3://$bucket/$key")

  /** Create a URI for a cluster log.
    */
  def logUri(cluster: Cluster): URI = uriOf(s"logs/${cluster.name}")

  /** Test whether or not a key exists.
    */
  def exists(key: String): IO[Boolean] = IO {
    s3.keyExists(bucket, key)
  }

  /** Upload a string to S3 in a particular bucket.
    */
  def put(key: String, text: String): IO[PutObjectResponse] = IO {
    val req = PutObjectRequest.builder.bucket(bucket).key(key).build
    
    s3.putObject(req, RequestBody.fromString(text))
  }

  /** Upload a resource file to S3 (using a matching key) and return a URI to it.
    */
  def upload(resource: String, dirKey: String = "resources"): IO[URI] = {
    val key = s"""$dirKey/${resource.stripPrefix("/")}"""

    //An IO that will produce the contents of the classpath resource at `resource` as a string,
    //and will close the InputStream backed by the resource when reading the resource's data is
    //done, either successfully or due to an error.
    val contentsIo: IO[String] = {
      val streamIo = IO(getClass.getClassLoader.getResourceAsStream(resource))

      // load the contents of the file, treat as text, ensure unix line-endings
      def getContents(stream: InputStream): IO[String] =
        IO(Source.fromInputStream(stream).mkString.replace("\r\n", "\n"))

      // close the stream to free resources
      def closeStream(stream: InputStream): IO[Unit] =
        IO(stream.close())

      // open, load, and ensure closed
      streamIo.bracket(getContents)(closeStream)
    }

    for {
      _ <- IO(logger.debug(s"Uploading $resource to S3..."))
      // load the resource in the IO context
      contents <- contentsIo
      // upload it
      _ <- put(key, contents)
    } yield {
      uriOf(key)
    }
  }

  /** Fetch a file from an S3 bucket (does not download content).
    */
  def get(key: String): IO[ResponseInputStream[_]] = IO {
    val req = GetObjectRequest.builder.bucket(bucket).key(key).build
    
    s3.getObject(req)
  }

  /** Returns the canonical URL for a given key.
    */
  def publicUrlOf(key: String): String = {
    val req = GetUrlRequest.builder.bucket(bucket).key(key).build
                           
    s3.utilities.getUrl(req).toExternalForm
  }

  /** Delete a key from S3.
    */
  def rm(key: String): IO[Unit] = IO {
    val req = DeleteObjectRequest.builder.bucket(bucket).key(key).build
    
    s3.deleteObject(req)
    
    ()
  }

  /** List all the keys in a given S3 folder.
    */
  def ls(key: String, excludeSuccess: Boolean = false): IO[Seq[String]] = IO {
    val keys = s3.listKeys(bucket, key)

    // optionally filter out _SUCCESS files
    if (excludeSuccess) keys.filterNot(_.endsWith("/_SUCCESS")) else keys
  }

  /** Delete (recursively) all the keys under a given key from S3.
    */
  def rmdir(key: String)
           (implicit contextShift: ContextShift[IO] = Implicits.Defaults.contextShift): IO[Seq[String]] = {
    
    val ios = for (listing <- s3.listingsIterable(bucket, key).asScala) yield {
      if (listing.isEmpty) {
        IO(Nil)
      } else {
        val keys = listing.keys
        val objectsToDelete = keys.map(ObjectIdentifier.builder.key(_).build)
        val delete = Delete.builder.objects(objectsToDelete.asJava).build 
        val request = DeleteObjectsRequest.builder.bucket(bucket).delete(delete).build
        
        for {
          _ <- IO(logger.debug(s"Deleting ${keys.head} + ${keys.tail.size} more keys..."))
          _ <- IO(s3.deleteObjects(request))
        } yield keys
      }
    }

    // all the delete operations can happen in parallel
    ios.toList.parSequence.map(_.flatten)
  }

  /** Create a object to be used as a folder in S3.
    */
  def mkdir(key: String, metadata: String)
           (implicit contextShift: ContextShift[IO] = Implicits.Defaults.contextShift): IO[PutObjectResponse] = {
    
    logger.debug(s"Creating pseudo-dir '$key'")

    for {
      _        <- rmdir(key)
      response <- put(s"$key/metadata", metadata)
    } yield response
  }

  /** Create a new cluster with some initial job steps and return the job
    * flow response, which can be used to add additional steps later.
    */
  def createCluster(cluster: Cluster, steps: Seq[JobStep]): IO[RunJobFlowResponse] = {
    val bootstrapConfigs = cluster.bootstrapScripts.map(_.config)
    val allSteps         = cluster.bootstrapSteps ++ steps

    // create all the instances
    val instances = JobFlowInstancesConfig.builder
      .additionalMasterSecurityGroups(config.emr.securityGroupIds.map(_.value): _*)
      .additionalSlaveSecurityGroups(config.emr.securityGroupIds.map(_.value): _*)
      .ec2SubnetId(config.emr.subnetId.value)
      .ec2KeyName(config.emr.sshKeyName)
      .keepJobFlowAliveWhenNoSteps(cluster.keepAliveWhenNoSteps)
      .instanceGroups(cluster.instanceGroups.asJava)
      .keepJobFlowAliveWhenNoSteps(true)
      .build

    // create the request for the cluster
    val baseRequestBuilder = RunJobFlowRequest.builder
      .name(cluster.name)
      .bootstrapActions(bootstrapConfigs.asJava)
      .applications(cluster.applications.map(_.application).asJava)
      .configurations(cluster.configurations.map(_.configuration).asJava)
      .releaseLabel(config.emr.releaseLabel.value)
      .serviceRole(config.emr.serviceRoleId.value)
      .jobFlowRole(config.emr.jobFlowRoleId.value)
      .autoScalingRole(config.emr.autoScalingRoleId.value)
      .logUri(logUri(cluster).toString)
      .visibleToAllUsers(cluster.visibleToAllUsers)
      .instances(instances)
      .steps(allSteps.map(_.config).asJava)

    // use a custom AMI
    val requestBuilder = cluster.amiId match {
      case Some(id) => baseRequestBuilder.customAmiId(id.value)
      case None     => baseRequestBuilder
    }

    // create the request
    val request = requestBuilder.build

    // create the IO action to launch the instance
    IO(emr.runJobFlow(request))
  }

  /** Spin up a set of clusters from a definition along with a set of jobs
    * that need to run (in any order!). As a cluster becomes available
    * steps from the various jobs will be sent to it for running.
    */
  def runJobs(cluster: Cluster, jobs: Seq[Seq[JobStep]], maxParallel: Int = 5)(implicit timer: Timer[IO] = Implicits.Defaults.timer): IO[Unit] = {
    val jobList = Random.shuffle(jobs).toList
    val totalSteps = jobList.flatten.size

    // determine the initial jobs to spin up each cluster with
    val (initialJobs, remainingJobs) = jobList.splitAt(maxParallel)

    // seconds to wait between cluster status checks and min pending steps per cluster
    val pollPeriod = 2.minutes
    val stepsPerJobFlow = 2

    // total number of steps completed
    var lastStepsCompleted = 0

    // create a cluster for each initial, parallel job
    initialJobs.map(job => createCluster(cluster, job)).sequence.flatMap { clusters =>
      val jobsQueue = scala.collection.mutable.Queue(remainingJobs: _*)

      // take the next job in the queue and add it to the cluster
      def addStepsToCluster(cluster: RunJobFlowResponse, nSteps: Int = 1): AddJobFlowStepsResponse = {
        var steps = jobsQueue.dequeue()

        // keep popping jobs until the minimum number of steps is reached
        while(steps.size < nSteps && jobsQueue.nonEmpty) {
          steps = steps ++ jobsQueue.dequeue()
        }

        // add all the steps via a single request
        val req = AddJobFlowStepsRequest.builder
          .jobFlowId(cluster.jobFlowId)
          .steps(steps.map(_.config).asJava)
          .build

        // get the list of steps added
        emr.addJobFlowSteps(req)
      }

      // check status and perform action for each cluster
      val processClusters: IO[Int] = {
        val clusterActions = for (cluster <- clusters) yield {

          // first get the list of all steps in the job flow
          val stepsState = IO {
            val req = ListStepsRequest.builder.clusterId(cluster.jobFlowId).build
            val steps = emr.listStepsPaginator(req).steps.asScala

            // either left (failed step) or right (all steps)
            steps.foldLeft(Right(List.empty): Either[StepSummary, List[StepSummary]]) {
              case (Left(step), _)             => Left(step)
              case (_, step) if step.isFailure => Left(step)
              case (Right(steps), step)        => Right(steps :+ step)
            }
          }

          // cluster action based on step state
          stepsState.flatMap {
            case Left(failedStep) => IO.raiseError(new Exception(failedStep.stopReason))
            case Right(steps) => IO {
              val pending = steps.count(_.isPending)

              // always keep steps pending in the cluster...
              if (pending < stepsPerJobFlow && jobsQueue.nonEmpty) {
                logger.debug(s"Adding job step(s) to ${cluster.jobFlowId}.")

                // add multiple steps per request to ensure rate limit isn't exceeded
                addStepsToCluster(cluster, stepsPerJobFlow - pending)
              }

              // return the total number of steps completed
              steps.count(_.isComplete)
            }
          }
        }

        // perform all the actions serially, sum the steps completed
        clusterActions.sequence.map(_.sum)
      }

      // repeatedly wait (see AWS poll rate limiting!) and then process the clusters
      def processJobQueue(): IO[Unit] = {
        (IO.sleep(pollPeriod) >> processClusters).flatMap { stepsCompleted =>
          if (stepsCompleted > lastStepsCompleted) {
            logger.info(s"Job queue progress: $stepsCompleted/$totalSteps steps complete.")
            lastStepsCompleted = stepsCompleted
          }

          // recurse until complete
          if (stepsCompleted < totalSteps) processJobQueue() else IO.unit
        }
      }

      // called after processing (whether error or success) to terminate all the clusters
      val terminateClusters: IO[Unit] = IO {
        val flowIds = clusters.map(_.jobFlowId).asJava
        val req = TerminateJobFlowsRequest.builder.jobFlowIds(flowIds).build

        emr.terminateJobFlows(req)
        ()
      }

      // wait until the queue is done or an error
      for {
        _ <- IO(logger.info(s"${clusters.size} job flows created; 0/$totalSteps steps complete."))
        _ <- processJobQueue().guaranteeCase {
          case ExitCase.Error(e) => terminateClusters >> IO.raiseError(e)
          case _                 => terminateClusters
        }
      } yield ()
    }
  }

  /** Helper: create a single job.
    */
  def runJob(cluster: Cluster, steps: JobStep*): IO[Unit] = runJobs(cluster, Seq(steps))
}
