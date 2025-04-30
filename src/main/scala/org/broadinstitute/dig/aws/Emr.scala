package org.broadinstitute.dig.aws

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dig.aws.config.EmrConfig
import org.broadinstitute.dig.aws.emr.{ClusterDef, Job}
import org.broadinstitute.dig.aws.emr.configurations.Configuration

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Random, Success, Try}
import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.services.emr.EmrClient
import software.amazon.awssdk.services.emr.model.{AddJobFlowStepsRequest, JobFlowInstancesConfig, ListStepsRequest, RunJobFlowRequest, RunJobFlowResponse, StepState, TerminateJobFlowsRequest}

import scala.collection.mutable

/** AWS client for creating EMR clusters and running jobs.
  */
object Emr extends LazyLogging {

  /** AWS SDK client. All runners can share a single client. */
  lazy val client: EmrClient = EmrClient.builder.build

  /** Runners launch and add steps to job clusters. */
  final class Runner(config: EmrConfig, logBucket: String) {
    private val subnetIterator = Iterator.continually(config.subnetIds).flatten

    private def createCluster(clusterDef: ClusterDef, env: Map[String, String]): RunJobFlowResponse = {
      val bootstrapConfigs = clusterDef.bootstrapScripts.map(_.config)
      val logUri           = s"s3://$logBucket/logs/${clusterDef.name}"
      var configurations   = clusterDef.applicationConfigurations
      var stepConcurrency  = clusterDef.stepConcurrency

      if (clusterDef.bootstrapSteps.nonEmpty && stepConcurrency > 1) {
        logger.warn("Bootstrap steps cannot run concurrently; disabling step concurrency")
        stepConcurrency = 1
      }

      configurations.find(_.classification == "hadoop-env") match {
        case Some(config) => config.export(env)
        case _            => configurations :+= new Configuration("hadoop-env").export(env)
      }

      val modifiedEnv = env.map { case (key, value) => "spark.yarn.appMasterEnv." + key -> value }
      configurations.find(_.classification == "spark-defaults") match {
        case Some(config) => config.addProperties(modifiedEnv)
        case _            => configurations :+= new Configuration("spark-defaults").addProperties(modifiedEnv)
      }

      val instances = JobFlowInstancesConfig.builder
        .additionalMasterSecurityGroups(config.securityGroupIds.map(_.value): _*)
        .additionalSlaveSecurityGroups(config.securityGroupIds.map(_.value): _*)
        .ec2SubnetId(subnetIterator.next().value)
        .ec2KeyName(config.sshKeyName)
        .instanceGroups(clusterDef.instanceGroups.asJava)
        .keepJobFlowAliveWhenNoSteps(true)
        .build

      val baseRequestBuilder = RunJobFlowRequest.builder
        .name(clusterDef.name)
        .bootstrapActions(bootstrapConfigs.asJava)
        .applications(clusterDef.applications.map(_.application).asJava)
        .configurations(configurations.map(_.build).asJava)
        .releaseLabel(clusterDef.releaseLabel.value)
        .serviceRole(config.serviceRoleId.value)
        .jobFlowRole(config.jobFlowRoleId.value)
        .autoScalingRole(config.autoScalingRoleId.value)
        .visibleToAllUsers(clusterDef.visibleToAllUsers)
        .logUri(logUri)
        .instances(instances)
        .steps(clusterDef.bootstrapSteps.map(_.build(true)).asJava)
        .stepConcurrencyLevel(stepConcurrency)

      val requestBuilder = clusterDef.amiId match {
        case Some(id) => baseRequestBuilder.customAmiId(id.value)
        case None     => baseRequestBuilder
      }

      val request = requestBuilder.build
      client.runJobFlow(request)
    }

    private def clusterStatus(cluster: RunJobFlowResponse, stepIds: Seq[String]): List[String] = {
      val req = ListStepsRequest.builder
        .clusterId(cluster.jobFlowId)
        .stepIds(stepIds.asJava)
        .build

      client.listStepsPaginator(req).steps.asScala.toArray.collect { step =>
        step.status.state match {
          case StepState.PENDING | StepState.RUNNING => step.id
          case StepState.FAILED                      => throw new Exception(s"${cluster.jobFlowId} failed")
          case StepState.CANCELLED                   => throw new Exception(s"${cluster.jobFlowId} cancelled")
          case _                                     => ""  // ignore other states
        }
      }.filter(_.nonEmpty).toList
    }

    /** Terminate a list of running clusters. */
    private def terminateClusters(clusters: Seq[RunJobFlowResponse]): Unit = {
      clusters.map(_.jobFlowId).sliding(10, 10).foreach { flowIds =>
        val req = TerminateJobFlowsRequest.builder.jobFlowIds(flowIds.asJava).build
        client.terminateJobFlows(req)
      }

      logger.info("Clusters terminated.")
    }

    /** Modified runJobs that terminates a cluster as soon as all its work is complete. */
    def runJobs(clusterDef: ClusterDef, env: Map[String, String], jobs: Seq[Job], maxParallel: Int = 5): Unit = {
      val allJobs = jobs.flatMap {
        case job if job.parallelSteps => job.steps.map(new Job(_))
        case job                      => Seq(job)
      }
      val maxActiveSteps     = 10
      val nClusters          = allJobs.size.min(maxParallel)
      val totalSteps         = jobs.flatMap(_.steps).size
      val terminateOnFailure = clusterDef.stepConcurrency == 1 || clusterDef.bootstrapSteps.nonEmpty

      logger.info(s"Creating $nClusters clusters for ${jobs.size} jobs...")

      val clusters: Vector[RunJobFlowResponse] = (1 to nClusters).toVector.map { _ =>
        Thread.sleep(1.second.toMillis) // delay to avoid rate limiting
        createCluster(clusterDef, env)
      }
      logger.info("Clusters launched.")

      // For each cluster, maintain a mutable queue of steps remaining and the currently active step ids.
      val stepQueues    = mutable.Map.empty[String, List[() => software.amazon.awssdk.services.emr.model.StepConfig]]
      val activeSteps   = mutable.Map.empty[String, List[String]]
      clusters.foreach { cluster =>
        // Distribute the shuffled steps across clusters.
        // Here we take the overall shuffled list and assign them round-robin.
        val stepsForThisCluster =
          Random.shuffle(allJobs).zipWithIndex.collect { case (job, idx) if idx % nClusters == clusters.indexOf(cluster) => job }
        // We extract the underlying step builders (by deferring the build so we can set the termination flag later).
        val stepBuilders = stepsForThisCluster.flatMap(_.steps).map { step =>
          // We wrap the build in a function so we can pass the flag at the right moment.
          () => step.build(terminateOnFailure)
        }.toList
        stepQueues(cluster.jobFlowId) = stepBuilders
        activeSteps(cluster.jobFlowId) = List.empty
      }

      // A mutable set of "live" clusters (identified by jobFlowId) that haven’t yet been terminated.
      val liveClusters = mutable.Set(clusters.map(_.jobFlowId): _*)

      // For progress reporting (global across clusters)
      var lastCompletedSteps = -1

      // Main loop: as long as there is any cluster still alive, poll them.
      while (liveClusters.nonEmpty) {
        liveClusters.foreach { jobFlowId =>
          // if there are steps queued or active for the cluster,
          // look them up and process on a per-cluster basis.
          val queue   = stepQueues.getOrElse(jobFlowId, Nil)
          var actives = activeSteps.getOrElse(jobFlowId, Nil)

          // Only poll status if there are active steps.
          if (actives.nonEmpty) {
            val cluster = clusters.find(_.jobFlowId == jobFlowId).get
            try {
              actives = clusterStatus(cluster, actives)
              activeSteps(jobFlowId) = actives
            } catch {
              case ex: AwsServiceException if ex.isThrottlingException =>
                logger.warn("AWS rate limit exceeded, throttling...")
                Thread.sleep(2.minutes.toMillis)
            }
          }

          // If there is capacity for more steps (AWS limit 10)
          if (actives.length < maxActiveSteps && queue.nonEmpty) {
            val remainingCapacity = maxActiveSteps - actives.length
            val (toAdd, remainingQueue) = queue.splitAt(remainingCapacity)
            val stepConfigs = toAdd.map(buildFn => buildFn())
            val cluster = clusters.find(_.jobFlowId == jobFlowId).get
            val req = AddJobFlowStepsRequest.builder
              .jobFlowId(cluster.jobFlowId)
              .steps(stepConfigs.asJava)
              .build
            val response = client.addJobFlowSteps(req)
            if (response.hasStepIds) {
              activeSteps(jobFlowId) = activeSteps(jobFlowId) ++ response.stepIds.asScala
            }
            stepQueues(jobFlowId) = remainingQueue
          }

          if (stepQueues.getOrElse(jobFlowId, Nil).isEmpty && activeSteps.getOrElse(jobFlowId, Nil).isEmpty) {
            logger.info(s"Terminating cluster $jobFlowId because all assigned work has been distributed and completed.")
            val terminateReq = TerminateJobFlowsRequest.builder.jobFlowIds(jobFlowId).build
            client.terminateJobFlows(terminateReq)
            liveClusters.remove(jobFlowId)
          }
          Thread.sleep(5.seconds.toMillis)
        }

        val nActive = activeSteps.values.map(_.length).sum
        val nQueued = stepQueues.values.map(_.length).sum
        val completed = totalSteps - (nActive + nQueued)
        if (completed > lastCompletedSteps) {
          logger.info(s"Global job progress: $completed/$totalSteps steps (${completed * 100 / totalSteps}%)")
          lastCompletedSteps = completed
        }
      }
      logger.info("All clusters have terminated their work.")
    }

    def runJob(clusterDef: ClusterDef, env: Map[String, String], job: Job): Unit = {
      runJobs(clusterDef, env, Seq(job))
    }
  }
}
