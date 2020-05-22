package org.broadinstitute.dig.aws

import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.io.Source
import scala.util.{Failure, Random, Success, Try}

import org.broadinstitute.dig.aws.config.AwsConfig
import org.broadinstitute.dig.aws.emr.ClusterDef

import com.typesafe.scalalogging.LazyLogging

import software.amazon.awssdk.core.ResponseInputStream
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
import software.amazon.awssdk.services.s3.model.{Delete, DeleteObjectRequest, DeleteObjectsRequest, GetObjectRequest, GetObjectResponse, GetUrlRequest, NoSuchKeyException, ObjectIdentifier, PutObjectRequest, PutObjectResponse, S3Object}

/** AWS controller (S3 + EMR clients).
  */
final class AWS(config: AwsConfig) extends LazyLogging {
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
  def logUri(cluster: ClusterDef): URI = uriOf(s"logs/${cluster.name}")

  /** Test whether or not a key exists.
    */
  def exists(key: String): Boolean = s3.keyExists(bucket, key)

  /** Upload a string to S3 in a particular bucket.
    */
  def put(key: String, text: String): PutObjectResponse = doPut(key, RequestBody.fromString(text))

  /** Upload the contents of a file to S3 in a particular bucket.
   */
  def put(key: String, file: Path): PutObjectResponse = doPut(key, RequestBody.fromFile(file))

  private def doPut(key: String, requestBody: RequestBody): PutObjectResponse = {
    val req = PutObjectRequest.builder.bucket(bucket).key(key).build
     
    s3.putObject(req, requestBody)
   }

  /** Upload a resource file to S3 (using a matching key) and return a URI to it.
    */
  def upload(resource: String, dirKey: String = "resources"): URI = {
    val key = s"""$dirKey/${resource.stripPrefix("/")}"""

    //An IO that will produce the contents of the classpath resource at `resource` as a string,
    //and will close the InputStream backed by the resource when reading the resource's data is
    //done, either successfully or due to an error.
    val stream = getClass.getClassLoader.getResourceAsStream(resource)

    // load the contents of the file, treat as text, ensure unix line-endings
    val contents = Source.fromInputStream(stream).mkString.replace("\r\n", "\n")

    // close the stream to free resources
    stream.close()

    logger.debug(s"Uploading $resource to S3...")
    put(key, contents)
    uriOf(key)
  }
  
  /** Download the data at an s3 key to a path, optionally overwriting any file that already exists
   *  at that path.  
   */
  def download(key: String, dest: Path, overwrite: Boolean = false): Unit = {
    if(overwrite) {
      Try {
        Files.delete(dest)
      }
    }

    val req = GetObjectRequest.builder.bucket(bucket).key(key).build
    
    s3.getObject(req, dest)
    ()
  }
  
  def eTagOf(key: String): Option[String] = {
    def stripQuotes(s: String): String = {
      val withoutLeadingQuote = if(s.startsWith("\"")) s.drop(1) else s
      
      if(withoutLeadingQuote.endsWith("\"")) withoutLeadingQuote.dropRight(1) else withoutLeadingQuote
    }
    
    getMetadataField(key)(response => stripQuotes(response.eTag))
  }
  
  def lastModifiedTimeOf(key: String): Option[Instant] = getMetadataField(key)(_.lastModified)
  
  private def getMetadataField[A](key: String)(field: GetObjectResponse => A): Option[A] = {
    val req = GetObjectRequest.builder.bucket(bucket).key(key).range("0-0").build
    
    Try(s3.getObject(req)) match {
      case Success(responseStream) =>
        try {
          Option(field(responseStream.response))
        } finally {
          responseStream.close()
        }
      case Failure(_: NoSuchKeyException) => None 
      case Failure(e) => throw e 
    }
  }

  /** Fetch a file from an S3 bucket (does not download content).
    */
  def get(key: String): ResponseInputStream[_] = {
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
  def rm(key: String): Unit = {
    val req = DeleteObjectRequest.builder.bucket(bucket).key(key).build
    
    s3.deleteObject(req)
    ()
  }

  /** List all the objects in a given S3 folder.
    */
  def ls(key: String, excludeSuccess: Boolean = false): Seq[S3Object] = {
    val objects = s3.listObjects(bucket, key)

    // optionally filter out _SUCCESS files
    if (excludeSuccess) objects.filterNot(_.key.endsWith("/_SUCCESS")) else objects
  }

  /** Delete (recursively) all the keys under a given key from S3.
    */
  def rmdir(key: String): Seq[String] = {
    s3.listingsIterable(bucket, key).asScala.flatMap { listing =>
      val keys = listing.keys
      val objectsToDelete = keys.map(ObjectIdentifier.builder.key(_).build)
      val delete = Delete.builder.objects(objectsToDelete.asJava).build
      val request = DeleteObjectsRequest.builder.bucket(bucket).delete(delete).build

      logger.debug(s"Deleting ${keys.head} + ${keys.tail.size} more keys...")
      s3.deleteObjects(request)
      keys
    }.toSeq
  }

  /** Create a object to be used as a folder in S3.
    */
  def mkdir(key: String, metadata: String): PutObjectResponse = {
    logger.debug(s"Creating pseudo-dir '$key'")

    rmdir(key)
    put(s"$key/metadata", metadata)
  }

  /** Create a new cluster with some initial job steps and return the job
    * flow response, which can be used to add additional steps later.
    */
  def createCluster(cluster: ClusterDef, steps: Seq[JobStep]): RunJobFlowResponse = {
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
    emr.runJobFlow(request)
  }

  /** Gets the current state of a cluster. It's either Left with the step that failed or
    * Right with a list of all the steps completed or running/pending.
    */
  def clusterState(cluster: RunJobFlowResponse): Either[StepSummary, List[StepSummary]] = {
    val req = ListStepsRequest.builder.clusterId(cluster.jobFlowId).build
    val steps = emr.listStepsPaginator(req).steps.asScala

    // either left (failed step) or right (all steps)
    steps.foldLeft(Right(List.empty): Either[StepSummary, List[StepSummary]]) {
      case (Left(step), _)              => Left(step)
      case (_, step) if step.isFailure  => Left(step)
      case (_, step) if step.isCanceled => Left(step)
      case (Right(steps), step)         => Right(steps :+ step)
    }
  }

  /** Adds additional steps to an already existing cluster.
    */
  def addStepsToCluster(cluster: RunJobFlowResponse, steps: Seq[JobStep]): AddJobFlowStepsResponse = {
    val req = AddJobFlowStepsRequest.builder
      .jobFlowId(cluster.jobFlowId)
      .steps(steps.map(_.config).asJava)
      .build

    // get the list of steps added
    emr.addJobFlowSteps(req)
  }

  /** Terminate a cluster.
    */
  def terminateClusters(clusters: Seq[RunJobFlowResponse]): Unit = {
    val flowIds = clusters.map(_.jobFlowId).asJava
    val req = TerminateJobFlowsRequest.builder.jobFlowIds(flowIds).build

    emr.terminateJobFlows(req)
    ()
  }

  /** Spin up a set of clusters from a definition along with a set of jobs
    * that need to run (in any order!). As a cluster becomes available
    * steps from the various jobs will be sent to it for running.
    */
  def runJobs(cluster: ClusterDef, jobs: Seq[Seq[JobStep]], maxParallel: Int = 5): Unit = {
    val jobList = Random.shuffle(jobs).toList
    val totalSteps = jobList.flatten.size

    // determine the initial jobs to spin up each cluster with
    val (initialJobs, remainingJobs) = jobList.splitAt(maxParallel)

    // pause between status checks, min pending steps per cluster
    val pollPeriod = 2.minutes
    val pendingStepsPerJobFlow = 2

    // create a cluster for each initial, parallel job
    val clusters = initialJobs.map(job => createCluster(cluster, job))

    // queue of the remaining jobs and count of completed steps
    var stepQueue = remainingJobs.flatten
    var lastStepsCompleted = 0

    // indicate how many steps are being distributed across clusters
    logger.info(s"${clusters.size} job flows created; 0/$totalSteps steps complete.")

    // loop until all jobs are complete
    try {
      while (true) {
        // wait between cluster polling so we don't hit the AWS limit
        Thread.sleep(pollPeriod.toMillis)

        // update the progress of each cluster
        val progress = for (cluster <- clusters) yield {
          clusterState(cluster) match {
            case Left(failedStep) => throw new Exception(failedStep.stopReason)
            case Right(steps) =>
              val pending = steps.count(_.isPending)

              // always keep steps pending in the cluster...
              if (pending < pendingStepsPerJobFlow && remainingJobs.nonEmpty) {
                val (steps, rest) = stepQueue.splitAt(pendingStepsPerJobFlow - pending)

                // add multiple steps per request to ensure rate limit isn't exceeded
                addStepsToCluster(cluster, steps)
                stepQueue = rest
              }

              // count the completed steps
              steps.count(_.isComplete)
          }
        }

        // show progress of the job queue if it has improved
        val stepsCompleted = progress.sum

        if (stepsCompleted > lastStepsCompleted) {
          logger.info(s"Job queue progress: $stepsCompleted/$totalSteps steps complete.")
          lastStepsCompleted = stepsCompleted
        }
      }
    }

    // always ensure the clusters are terminated
    finally {
      terminateClusters(clusters)
    }
  }

  /** Helper: create a single job.
    */
  def runJob(cluster: ClusterDef, steps: JobStep*): Unit = runJobs(cluster, Seq(steps))
}

object AWS {
  def bucketOf(uri: URI): String = uri.getHost
  def keyOf(uri: URI): String = uri.getPath
}
