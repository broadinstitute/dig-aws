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
import cats.effect.IO
import cats.effect.Timer
import cats.implicits._
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.emr.EmrClient
import software.amazon.awssdk.services.emr.model.JobFlowInstancesConfig
import software.amazon.awssdk.services.emr.model.ListStepsRequest
import software.amazon.awssdk.services.emr.model.RunJobFlowRequest
import software.amazon.awssdk.services.emr.model.RunJobFlowResponse
import software.amazon.awssdk.services.emr.model.StepSummary
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

  /** Upload a file to S3 in a particular bucket.
    */
  def put(key: String, stream: InputStream): IO[PutObjectResponse] = IO {
    val req = PutObjectRequest.builder.bucket(bucket).key(key).build
    
    s3.putObject(req, RequestBody.fromInputStream(stream, ???))
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
    val req = GetUrlRequest.builder
                           .bucket(bucket)
                           .key(key)
                           .build
                           
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

  /** Create a job request that will be used to create a new EMR cluster and
    * run a series of steps.
    */
  def runJob(cluster: Cluster, steps: Seq[JobStep]): IO[RunJobFlowResponse] = {
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
      
    val requestBuilder = cluster.amiId match {
      case Some(id) => baseRequestBuilder.customAmiId(id.value)
      case None     => baseRequestBuilder
    }
    
    val request = requestBuilder.build

    // create the IO action to launch the instance
    IO {
      val job = emr.runJobFlow(request)

      // show the cluster, job ID and # of total steps being executed
      logger.info(s"Starting ${cluster.name} as ${job.jobFlowId} with ${steps.size} steps")

      // return the job
      job
    }
  }

  /** Helper: create a job that's a single step.
    */
  def runJob(cluster: Cluster, step: JobStep): IO[RunJobFlowResponse] = runJob(cluster, Seq(step))

  /** Periodically send a request to the cluster to determine the state of all
    * steps in the job. Log output showing the % complete the jobs is or throw
    * an exception if the job failed or was interrupt/cancelled.
    */
  def waitForJob(
      job: RunJobFlowResponse, 
      stepsComplete: Int = 0)(implicit timer: Timer[IO] = Implicits.Defaults.timer): IO[RunJobFlowResponse] = {
    // wait a little bit then get the status of all steps in the job
    val getStatus = for (_ <- IO.sleep(60.seconds)) yield {
      val req = ListStepsRequest.builder.clusterId(job.jobFlowId).build

      // extract the steps from the request response; aws reverses them
      val steps = emr.listSteps(req).steps.asScala.reverse

      // count all the completes, failures, etc.
      steps.foldLeft(Right(0, steps.size): Either[StepSummary, (Int, Int)]) {
        case (Left(step), _)                          => Left(step)
        case (Right((n, m)), step) if step.isComplete => Right(n + 1, m)
        case (_, step) if step.isStopped              => Left(step)
        case (x, _)                                   => x
      }
    }

    // get the status, log appropriately, and continue or stop
    getStatus.flatMap {
      case Left(step) =>
        logger.error(s"Job ${job.jobFlowId} failed: ${step.stopReason}.")

        // terminate the program
        IO.raiseError(new Exception(step.stopReason))

      case Right((n, m)) =>
        if (n != stepsComplete) {
          logger.info(s"Job ${job.jobFlowId} progress: $n/$m steps complete.")
        }

        // return the job on completion or continue waiting...
        if (n == m) IO(job) else waitForJob(job, n)
    }
  }

  /**
    * Given a sequence of jobs, run them in parallel, but limit the maximum
    * concurrency so too many clusters aren't created at once.
    */
  def waitForJobs(jobs: Seq[IO[RunJobFlowResponse]], maxClusters: Int = 5)
                 (implicit contextShift: ContextShift[IO] = Implicits.Defaults.contextShift): IO[Unit] = {
    Utils.waitForTasks(jobs, maxClusters) { job =>
      job.flatMap(waitForJob(_))
    }
  }

  /** Often times there are N jobs that are all identical (aside from command
    * line parameters) that need to be run, and can be run in parallel.
    *
    * This can be done by spinning up a unique cluster for each, but has the
    * downside that the provisioning step (which can take several minutes) is
    * run for each job.
    *
    * This function allows a list of "jobs" (read: a list of a list of steps)
    * to be passed, and N clusters will be made that will run through all the
    * jobs until complete. This way the provisioning costs are only paid for
    * once.
    *
    * This should only be used if all the jobs can be run in parallel.
    *
    * NOTE: The jobs are shuffled so that jobs that may be large and clumped
    *       together won't happen every time the jobs run together.
    */
  def clusterJobs(cluster: Cluster, jobs: Seq[Seq[JobStep]], maxClusters: Int = 5): Seq[IO[RunJobFlowResponse]] = {
    val indexedJobs = Random.shuffle(jobs).zipWithIndex.map {
      case (job, i) => (i % maxClusters, job)
    }

    // bootstrap steps + job steps
    val totalSteps = cluster.bootstrapSteps.size * maxClusters + jobs.flatten.size

    // AWS limit of 256 steps per job cluster
    require(totalSteps <= maxClusters * 256)

    // round-robin each job into a cluster
    val clusteredJobs = indexedJobs.groupBy(_._1).mapValues(_.map(_._2))

    // for each cluster, create a "job" that's all the steps appended
    clusteredJobs.values
      .map(jobs => runJob(cluster, jobs.flatten))
      .toSeq
  }
}
