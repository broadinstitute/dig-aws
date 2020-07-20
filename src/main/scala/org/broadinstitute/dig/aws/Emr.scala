package org.broadinstitute.dig.aws

import com.typesafe.scalalogging.LazyLogging

import org.broadinstitute.dig.aws.config.EmrConfig
import org.broadinstitute.dig.aws.emr.ClusterDef
import org.broadinstitute.dig.aws.emr.configurations.Configuration

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Random

import software.amazon.awssdk.services.emr.EmrClient
import software.amazon.awssdk.services.emr.model.{AddJobFlowStepsRequest, AddJobFlowStepsResponse, JobFlowInstancesConfig, ListStepsRequest, RunJobFlowRequest, RunJobFlowResponse, StepState, StepSummary, TerminateJobFlowsRequest}

/** AWS client for creating EMR clusters and running jobs.
  */
object Emr extends LazyLogging {

  /** AWS SDK client. All runners can share a single client. */
  lazy val client: EmrClient = EmrClient.builder.build

  /** Runners launch and add steps to job clusters. */
  final class Runner(config: EmrConfig, logBucket: String) {

    /** Create a new cluster with some initial job steps and return the job
      * flow response, which can be used to add additional steps later.
      */
    def createCluster(cluster: ClusterDef, env: Map[String, String], steps: Seq[JobStep]): RunJobFlowResponse = {
      val bootstrapConfigs = cluster.bootstrapScripts.map(_.config)
      val allSteps = cluster.bootstrapSteps ++ steps
      val logUri = s"s3://$logBucket/logs/${cluster.name}"
      var configurations = cluster.applicationConfigurations

      // add environment variables both yarn (for PySpark) and hadoop (for Scripts)
      for (export <- Seq("yarn-env", "hadoop-env")) {
        configurations.find(_.classification == export) match {
          case Some(config) => config.export(env)
          case _            => configurations :+= new Configuration(export).export(env)
        }
      }

      // create all the instances
      val instances = JobFlowInstancesConfig.builder
        .additionalMasterSecurityGroups(config.securityGroupIds.map(_.value): _*)
        .additionalSlaveSecurityGroups(config.securityGroupIds.map(_.value): _*)
        .ec2SubnetId(config.subnetId.value)
        .ec2KeyName(config.sshKeyName)
        .instanceGroups(cluster.instanceGroups.asJava)
        .keepJobFlowAliveWhenNoSteps(true)
        .build

      // create the request for the cluster
      val baseRequestBuilder = RunJobFlowRequest.builder
        .name(cluster.name)
        .bootstrapActions(bootstrapConfigs.asJava)
        .applications(cluster.applications.map(_.application).asJava)
        .configurations(configurations.map(_.build).asJava)
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
      val steps = Utils.awsRetry() {
        client.listStepsPaginator(req).steps.asScala
      }

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
    def runJobs(cluster: ClusterDef, env: Map[String, String], jobs: Seq[Seq[JobStep]], pendingBatchSize: Int = 2, maxParallel: Int = 5): Unit = {
      val jobList = Random.shuffle(jobs).toList

      // determine the initial jobs to spin up each cluster with
      val (initialJobs, remainingJobs) = jobList.splitAt(maxParallel)

      // create a cluster for each initial, parallel job
      val clusters = initialJobs.map(job => createCluster(cluster, env, job))

      // calculate the total number of steps (including bootstrap steps)
      val totalSteps = jobList.flatten.size + (cluster.bootstrapSteps.size * clusters.size)

      // queue of the remaining jobs and count of completed steps
      var jobQueue = remainingJobs
      var lastStepsCompleted = 0

      // the initial poll period is about how long it takes to provision a cluster
      val maxPollPeriod = 5.minutes
      var pollPeriod = maxPollPeriod

      // indicate how many steps are being distributed across clusters
      logger.info(s"${clusters.size} job flows created; $totalSteps steps")

      // loop until all jobs are complete
      try {
        val startTime = System.currentTimeMillis

        while (lastStepsCompleted < totalSteps) {
          // wait between cluster polling so we don't hit the AWS limit
          Thread.sleep(pollPeriod.toMillis)

          // reset the poll period back to the maximum
          pollPeriod = maxPollPeriod

          // update the progress of each cluster
          val progress = for (cluster <- clusters) yield {
            clusterState(cluster) match {
              case Left(failedStep) => throw new Exception(s"Job failed (step ${failedStep.id}); see: https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:${cluster.jobFlowId}")
              case Right(steps) =>
                val pending = steps.count(_.status.state == StepState.PENDING)

                // reduce the poll period to the minimum number of steps being waited on
                pollPeriod = pollPeriod.toMinutes.min(pending + 1).minutes

                // always keep steps pending in the cluster, but keep entire jobs together
                if (pending < pendingBatchSize && jobQueue.nonEmpty) {
                  val n = ((pendingBatchSize - pending) / jobQueue.head.size).max(1)

                  // take as many jobs as needed to reach pending size
                  val (jobs, rest) = jobQueue.splitAt(n)

                  // add multiple steps per request to ensure rate limit isn't exceeded
                  addStepsToCluster(cluster, jobs.flatten)
                  jobQueue = rest
                }

                // count the completed steps
                steps.count(_.status.state == StepState.COMPLETED)
            }
          }

          // get the total number of completed steps
          val stepsCompleted = progress.sum

          // log any updated progress
          if (stepsCompleted > lastStepsCompleted) {
            val pct = stepsCompleted * 100 / totalSteps
            val elapsed = (System.currentTimeMillis - startTime).milliseconds

            // calculate a pretty string for estimated time remaining
            val timeLeft = elapsed * (totalSteps - stepsCompleted) / stepsCompleted match {
              case d if d > 1.day    => d.toDays.days.toString
              case d if d > 1.hour   => d.toHours.hours.toString
              case d if d > 1.minute => d.toMinutes.minutes.toString
              case d                 => d.toSeconds.seconds.toString
            }

            // update the current progress
            logger.info(s"Job queue progress: $stepsCompleted/$totalSteps steps ($pct%); est. $timeLeft remaining")
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
    def runJob(cluster: ClusterDef, env: Map[String, String], steps: JobStep*): Unit = {
      runJobs(cluster, env, Seq(steps))
    }
  }
}
