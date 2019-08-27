package org.broadinstitute.dig.aggregator.core

import cats.effect._
import cats.implicits._

import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult
import com.amazonaws.services.elasticmapreduce.model.StepSummary
import com.amazonaws.services.elasticmapreduce._
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._

import com.typesafe.scalalogging.LazyLogging

import java.io.InputStream
import java.net.URI

import org.broadinstitute.dig.aws.config.AWSConfig
import org.broadinstitute.dig.aws.emr.Cluster

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.io.Source
import scala.util._
import org.broadinstitute.dig.aws.Implicits
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.Utils

/** AWS controller (S3 + EMR clients).
  */
final class AWS(config: AWSConfig) extends LazyLogging {
  import Implicits._

  /** The same region and bucket are used for all operations.
    */
  val bucket: String = config.s3.bucket

  /** S3 client for storage.
    */
  val s3: AmazonS3 = AmazonS3ClientBuilder.standard.build

  /** EMR client for running map/reduce jobs.
    */
  val emr: AmazonElasticMapReduce = AmazonElasticMapReduceClientBuilder.standard.build

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
  def put(key: String, text: String): IO[PutObjectResult] = IO {
    s3.putObject(bucket, key, text)
  }

  /** Upload a file to S3 in a particular bucket.
    */
  def put(key: String, stream: InputStream): IO[PutObjectResult] = IO {
    s3.putObject(bucket, key, stream, new ObjectMetadata())
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
  def get(key: String): IO[S3Object] = IO {
    s3.getObject(bucket, key)
  }

  /** Returns the canonical URL for a given key.
    */
  def publicUrlOf(key: String): String = {
    s3.getUrl(bucket, key).toExternalForm
  }

  /** Delete a key from S3.
    */
  def rm(key: String): IO[Unit] = IO {
    s3.deleteObject(bucket, key)
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
    
    val ios = for (listing <- s3.listingsIterator(bucket, key)) yield {
      if (listing.getObjectSummaries.isEmpty) {
        IO(Nil)
      } else {
        val keys        = listing.keys
        val keyVersions = keys.map(new DeleteObjectsRequest.KeyVersion(_))
        val request     = new DeleteObjectsRequest(bucket).withKeys(keyVersions.asJava)

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
           (implicit contextShift: ContextShift[IO] = Implicits.Defaults.contextShift): IO[PutObjectResult] = {
    
    logger.debug(s"Creating pseudo-dir '$key'")

    for {
      _        <- rmdir(key)
      metadata <- put(s"$key/metadata", metadata)
    } yield metadata
  }

  /** Create a job request that will be used to create a new EMR cluster and
    * run a series of steps.
    */
  def runJob(cluster: Cluster, steps: Seq[JobStep]): IO[RunJobFlowResult] = {
    val bootstrapConfigs = cluster.bootstrapScripts.map(_.config)
    val allSteps         = cluster.bootstrapSteps ++ steps

    // create all the instances
    val instances = new JobFlowInstancesConfig()
      .withAdditionalMasterSecurityGroups(config.emr.securityGroupIds.map(_.value): _*)
      .withAdditionalSlaveSecurityGroups(config.emr.securityGroupIds.map(_.value): _*)
      .withEc2SubnetId(config.emr.subnetId.value)
      .withEc2KeyName(config.emr.sshKeyName)
      .withKeepJobFlowAliveWhenNoSteps(cluster.keepAliveWhenNoSteps)
      .withInstanceGroups(cluster.instanceGroups.asJava)

    // create the request for the cluster
    val request = new RunJobFlowRequest()
      .withName(cluster.name)
      .withBootstrapActions(bootstrapConfigs.asJava)
      .withApplications(cluster.applications.map(_.application).asJava)
      .withConfigurations(cluster.configurations.map(_.configuration).asJava)
      .withReleaseLabel(config.emr.releaseLabel.value)
      .withServiceRole(config.emr.serviceRoleId.value)
      .withJobFlowRole(config.emr.jobFlowRoleId.value)
      .withAutoScalingRole(config.emr.autoScalingRoleId.value)
      .withLogUri(logUri(cluster).toString)
      .withVisibleToAllUsers(cluster.visibleToAllUsers)
      .withInstances(instances)
      .withSteps(allSteps.map(_.config).asJava)

    // create the IO action to launch the instance
    IO {
      val job = cluster.amiId match {
        case Some(id) => emr.runJobFlow(request.withCustomAmiId(id.value))
        case None     => emr.runJobFlow(request)
      }

      // show the cluster, job ID and # of total steps being executed
      logger.info(s"Starting ${cluster.name} as ${job.getJobFlowId} with ${steps.size} steps")

      // return the job
      job
    }
  }

  /** Helper: create a job that's a single step.
    */
  def runJob(cluster: Cluster, step: JobStep): IO[RunJobFlowResult] = {
    runJob(cluster, Seq(step))
  }

  /** Periodically send a request to the cluster to determine the state of all
    * steps in the job. Log output showing the % complete the jobs is or throw
    * an exception if the job failed or was interrupt/cancelled.
    */
  def waitForJob(
      job: RunJobFlowResult, 
      stepsComplete: Int = 0)(implicit timer: Timer[IO] = Implicits.Defaults.timer): IO[RunJobFlowResult] = {
    // wait a little bit then get the status of all steps in the job
    val getStatus = for (_ <- IO.sleep(60.seconds)) yield {
      val req = new ListStepsRequest().withClusterId(job.getJobFlowId)

      // extract the steps from the request response; aws reverses them
      val steps = emr.listSteps(req).getSteps.asScala.reverse

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
        logger.error(s"Job ${job.getJobFlowId} failed: ${step.stopReason}.")

        // terminate the program
        IO.raiseError(new Exception(step.stopReason))

      case Right((n, m)) =>
        if (n != stepsComplete) {
          logger.info(s"Job ${job.getJobFlowId} progress: $n/$m steps complete.")
        }

        // return the job on completion or continue waiting...
        if (n == m) IO(job) else waitForJob(job, n)
    }
  }

  /**
    * Given a sequence of jobs, run them in parallel, but limit the maximum
    * concurrency so too many clusters aren't created at once.
    */
  def waitForJobs(jobs: Seq[IO[RunJobFlowResult]], maxClusters: Int = 5)
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
  def clusterJobs(cluster: Cluster, jobs: Seq[Seq[JobStep]], maxClusters: Int = 5): Seq[IO[RunJobFlowResult]] = {
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
