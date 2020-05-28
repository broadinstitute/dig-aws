package org.broadinstitute.dig.aws

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dig.aws.config.EmrConfig
import org.broadinstitute.dig.aws.emr.ClusterDef
import org.broadinstitute.dig.aws.emr.configurations.Yarn

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Random
import software.amazon.awssdk.services.emr.EmrClient
import software.amazon.awssdk.services.emr.model.{AddJobFlowStepsRequest, AddJobFlowStepsResponse, JobFlowInstancesConfig, ListStepsRequest, RunJobFlowRequest, RunJobFlowResponse, StepState, StepSummary, TerminateJobFlowsRequest}

/** AWS client for creating EMR clusters and running jobs.
  */
object Emr extends LazyLogging {

  /** AWS SDK client. All runners can share a single client. */
  val client: EmrClient = EmrClient.builder.build

  /** Runners launch and add steps to job clusters. */
  final class Runner(config: EmrConfig, logBucket: String) {

    /** Create a new cluster with some initial job steps and return the job
      * flow response, which can be used to add additional steps later.
      */
    def createCluster(cluster: ClusterDef, steps: Seq[JobStep]): RunJobFlowResponse = {
      val bootstrapConfigs = cluster.bootstrapScripts.map(_.config)
      val allSteps = cluster.bootstrapSteps ++ steps
      val logUri = s"s3://$logBucket/logs/${cluster.name}"

      // create all the instances
      val instances = JobFlowInstancesConfig.builder
        .additionalMasterSecurityGroups(config.securityGroupIds.map(_.value): _*)
        .additionalSlaveSecurityGroups(config.securityGroupIds.map(_.value): _*)
        .ec2SubnetId(config.subnetId.value)
        .ec2KeyName(config.sshKeyName)
        .keepJobFlowAliveWhenNoSteps(cluster.keepAliveWhenNoSteps)
        .instanceGroups(cluster.instanceGroups.asJava)
        .keepJobFlowAliveWhenNoSteps(true)
        .build

      // create the request for the cluster
      val baseRequestBuilder = RunJobFlowRequest.builder
        .name(cluster.name)
        .bootstrapActions(bootstrapConfigs.asJava)
        .applications(cluster.applications.map(_.application).asJava)
        .configurations(cluster.applicationConfigurations.map(_.build).asJava)
        .releaseLabel(config.releaseLabel.value)
        .serviceRole(config.serviceRoleId.value)
        .jobFlowRole(config.jobFlowRoleId.value)
        .autoScalingRole(config.autoScalingRoleId.value)
        .visibleToAllUsers(cluster.visibleToAllUsers)
        .logUri(logUri)
        .instances(instances)
        .steps(allSteps.map(_.config).asJava)

      // use a custom AMI
      val requestBuilder = cluster.amiId match {
        case Some(id) => baseRequestBuilder.customAmiId(id.value)
        case None => baseRequestBuilder
      }

      // create the request
      val request = requestBuilder.build

      // create the IO action to launch the instance
      client.runJobFlow(request)
    }

    /** Gets the current state of a cluster. It's either Left with the step that failed or
      * Right with a list of all the steps completed or running/pending.
      */
    def clusterState(cluster: RunJobFlowResponse): Either[StepSummary, List[StepSummary]] = {
      val req = ListStepsRequest.builder.clusterId(cluster.jobFlowId).build
      val steps = client.listStepsPaginator(req).steps.asScala

      // either left (failed step) or right (all steps)
      steps.foldLeft(Right(List.empty): Either[StepSummary, List[StepSummary]]) {
        case (Left(step), _) => Left(step)
        case (_, step) if step.status.state == StepState.FAILED => Left(step)
        case (_, step) if step.status.state == StepState.CANCELLED => Left(step)
        case (Right(steps), step) => Right(steps :+ step)
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
      client.addJobFlowSteps(req)
    }

    /** Terminate a cluster.
      */
    def terminateClusters(clusters: Seq[RunJobFlowResponse]): Unit = {
      val flowIds = clusters.map(_.jobFlowId).asJava
      val req = TerminateJobFlowsRequest.builder.jobFlowIds(flowIds).build

      client.terminateJobFlows(req)
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
        while (lastStepsCompleted < totalSteps) {
          // wait between cluster polling so we don't hit the AWS limit
          Thread.sleep(pollPeriod.toMillis)

          // update the progress of each cluster
          val progress = for (cluster <- clusters) yield {
            clusterState(cluster) match {
              case Left(failedStep) => throw new Exception(s"Job failed (step ${failedStep.id}); see: https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:${cluster.jobFlowId}")
              case Right(steps) =>
                val pending = steps.count(_.status.state == StepState.PENDING)

                // always keep steps pending in the cluster...
                if (pending < pendingStepsPerJobFlow && remainingJobs.nonEmpty) {
                  val (steps, rest) = stepQueue.splitAt(pendingStepsPerJobFlow - pending)

                  // add multiple steps per request to ensure rate limit isn't exceeded
                  addStepsToCluster(cluster, steps)
                  stepQueue = rest
                }

                // count the completed steps
                steps.count(_.status.state == StepState.COMPLETED)
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
    def runJob(cluster: ClusterDef, steps: JobStep*): Unit = {
      runJobs(cluster, Seq(steps))
    }
  }
}
