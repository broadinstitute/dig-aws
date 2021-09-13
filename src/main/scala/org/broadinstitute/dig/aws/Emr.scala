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
import software.amazon.awssdk.services.emr.model.{
  AddJobFlowStepsRequest,
  JobFlowInstancesConfig,
  ListStepsRequest,
  RunJobFlowRequest,
  RunJobFlowResponse,
  StepState,
  TerminateJobFlowsRequest
}

import scala.collection.compat.immutable.LazyList

/** AWS client for creating EMR clusters and running jobs.
  */
object Emr extends LazyLogging {

  /** AWS SDK client. All runners can share a single client. */
  lazy val client: EmrClient = EmrClient.builder.build

  /** Runners launch and add steps to job clusters. */
  final class Runner(config: EmrConfig, logBucket: String) {
    private val subnetIterator = Iterator.continually(config.subnetIds).flatten

    /** Create a new cluster with some initial job steps and return the job
      * flow response, which can be used to add additional steps later.
      */
    def createCluster(clusterDef: ClusterDef, env: Map[String, String]): RunJobFlowResponse = {
      val bootstrapConfigs = clusterDef.bootstrapScripts.map(_.config)
      val logUri           = s"s3://$logBucket/logs/${clusterDef.name}"
      var configurations   = clusterDef.applicationConfigurations
      var stepConcurrency  = clusterDef.stepConcurrency

      // cannot run bootstrap steps in parallel with other steps!!
      if (clusterDef.bootstrapSteps.nonEmpty && stepConcurrency > 1) {
        logger.warn("Bootstrap steps cannot run concurrently; disabling step concurrency")
        stepConcurrency = 1
      }

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
        .ec2SubnetId(subnetIterator.next().value)
        .ec2KeyName(config.sshKeyName)
        .instanceGroups(clusterDef.instanceGroups.asJava)
        .keepJobFlowAliveWhenNoSteps(true)
        .build

      // create the request for the cluster
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

      // use a custom AMI
      val requestBuilder = clusterDef.amiId match {
        case Some(id) => baseRequestBuilder.customAmiId(id.value)
        case None     => baseRequestBuilder
      }

      // create the request
      val request = requestBuilder.build

      // create the IO action to launch the instance
      client.runJobFlow(request)
    }

    /** Returns an array of active step IDs in a cluster. */
    def clusterStatus(cluster: RunJobFlowResponse, stepIds: Seq[String]): Array[String] = {
      val req = ListStepsRequest.builder
        .clusterId(cluster.jobFlowId)
        .stepIds(stepIds.asJava)
        .build

      // keep only the active or pending steps
      client.listStepsPaginator(req).steps.asScala.toArray.collect { step =>
        step.status.state match {
          case StepState.PENDING | StepState.RUNNING => step.id
          case StepState.FAILED                      => throw new Exception(s"${cluster.jobFlowId} failed")
          case StepState.CANCELLED                   => throw new Exception(s"${cluster.jobFlowId} cancelled")
        }
      }
    }

    /** Terminate a list of running clusters. */
    def terminateClusters(clusters: Seq[RunJobFlowResponse]): Unit = {
      clusters.map(_.jobFlowId).sliding(10, 10).foreach { flowIds =>
        val req = TerminateJobFlowsRequest.builder.jobFlowIds(flowIds.asJava).build
        client.terminateJobFlows(req)
      }

      logger.info("Clusters terminated.")
    }

    /** Spin up maxParallel clusters in order to execute jobs.
      *
      * The jobs are shuffled and grouped to clusters in a round-robin fashion. This
      * is done so that any pathological grouping of jobs that take a long time to
      * complete won't be paired together on the same cluster every time.
      *
      * Once the jobs are assigned to a cluster, the cluster is spun created with an
      * initial set of steps and then the remaining steps are added afterwards. AWS
      * only allows a cluster to be created with a limited number of steps.
      *
      * This function waits until all steps are completed before returning. This is
      * because AWS limits how many steps can be active on a cluster at one time. For
      * this reason, we need to periodically poll and queue more steps when a cluster
      * is able to receive them.
      */
    def runJobs(clusterDef: ClusterDef, env: Map[String, String], jobs: Seq[Job], maxParallel: Int = 5): Unit = {
      val allJobs = jobs.flatMap {
        case job if job.parallelSteps => job.steps.map(new Job(_))
        case job                      => Seq(job)
      }

      // this is an AWS limit in place when polling the status of steps
      val maxActiveSteps     = 10
      val nClusters          = allJobs.size.min(maxParallel)
      val totalSteps         = jobs.flatMap(_.steps).size
      val terminateOnFailure = clusterDef.stepConcurrency == 1 || clusterDef.bootstrapSteps.nonEmpty

      // indicate how many jobs are being distributed across clusters
      logger.info(s"Creating $nClusters clusters for ${jobs.size} jobs...")

      // spin up clusters
      val clusters = for (_ <- 1 to nClusters) yield {
        Thread.sleep(1.second.toMillis)

        // delay above needed to prevent rate limit issues
        createCluster(clusterDef, env)
      }

      // now run, but wrap so clusters will terminate if something goes wrong
      val runResult = Try {
        logger.info("Clusters launched.")

        /* At this point we evenly divide all the jobs among the clusters and
         * then flatten the steps. Each cluster (by index) will have its own
         * array of steps that can be taken from and updated in order to add
         * them to the cluster when it has available step space.
         *
         * It's important that each cluster have its own queue instead of a
         * single queue for all jobs because...
         *
         * 1. The steps of jobs need to be guaranteed to run serially, which
         *    means they must run on the same cluster.
         *
         * 2. In order to appropriately poll and not break the AWS rate limit,
         *    we need to always keep each cluster maximized to 10 steps. Jobs
         *    can have any number of steps (e.g. > 10), which means we can't
         *    just take an entire job from the queue and add it to the cluster.
         *
         * Ideally, there'd be a single queue for all jobs, and each cluster
         * would have both a queue of jobs and a queue of steps. If the step
         * queue of the cluster was low, it would pull a job from the job queue
         * and the job's steps would be added to the step queue until it was
         * ready to continue. This would alleviate the current problem of some
         * jobs taking considerably longer than others, causing some clusters
         * at the end to be sitting idle, doing nothing, while there are others
         * with jobs still pending.
         */
        val stepQueue = LazyList
          .continually(clusters)
          .flatten
          .zip(Random.shuffle(allJobs))
          .groupMap(_._1)(_._2)
          .toArray
          .map(_._2.flatMap(_.steps).toList)

        // quick sanity check...
        assert(stepQueue.map(_.length).sum == totalSteps)

        /* Steps taken from the stepQueue and added to a cluster have their step ids
         * added to a set of parallel arrays (one per cluster).
         */
        val activeSteps = clusters.map(_ => Array.empty[String]).toArray

        // cache of the last count of completed steps, so we don't spam the log
        var lastCompletedSteps = -1

        // loop until all step queues and active step arrays are empty
        while (stepQueue.exists(_.nonEmpty) || activeSteps.exists(_.nonEmpty)) {

          /* AWS rate limits their API. However, it appears as though this rate
           * limit is computed. This means that if (for example) the rate limit
           * is 300 requests per minute, then that's 5 requests per second. If
           * we make 6 requests in a second - even if we don't exceed the 300
           * requests per minute, AWS will reject the request and cause us to
           * throw an exception. For this reason, we have a few options:
           *
           *  1. Sleep between polls of all clusters.
           *  2. Sleep between polls of each individual cluster.
           *  3. Sleep between rate limit exceptions.
           *
           * Option (1) would mean "sleep for X minutes, then iterate over the
           * clusters and poll them". This is fine until the number of clusters
           * is above an unknown threshold at which point the rate limit is
           * exceeded.
           *
           * Option (2) would mean "iterate over the clusters, sleeping between
           * each iteration". This has the issue that if the delay and number
           * of clusters is high enough, the delay between polling each cluster
           * could be higher than with option (1).
           *
           * Option (3) means polling as often as possible, but when AWS fails
           * due to a rate limit error, sleep for X amount of time before we
           * continue polling again.
           *
           * None of these options are mutually exclusive.
           */

          try {
            //Thread.sleep(5.minutes.toMillis) // option (1)

            // poll each active cluster, add steps, etc.
            for ((cluster, i) <- clusters.zipWithIndex) {
              if (activeSteps(i).nonEmpty || stepQueue(i).nonEmpty) {
                Thread.sleep(10.seconds.toMillis) // option (2)

                // poll the cluster status only only if there are active steps
                if (activeSteps(i).nonEmpty) {
                  activeSteps(i) = clusterStatus(cluster, activeSteps(i).toIndexedSeq)
                }

                // if there are too few active steps, pull steps from the queue
                if (activeSteps(i).length < maxActiveSteps && stepQueue(i).nonEmpty) {
                  val n                            = maxActiveSteps - activeSteps(i).length
                  val (stepsToAdd, stepsRemaining) = stepQueue(i).splitAt(n)

                  // get the step configurations
                  val stepConfigs = stepsToAdd.map(_.build(terminateOnFailure))

                  // create the add steps request
                  val req = AddJobFlowStepsRequest.builder
                    .jobFlowId(cluster.jobFlowId)
                    .steps(stepConfigs.asJava)
                    .build

                  // add the steps to the cluster
                  val response = client.addJobFlowSteps(req)

                  // add new step ids to the set of active steps for this cluster
                  if (response.hasStepIds) {
                    activeSteps(i) ++= response.stepIds.asScala
                  }

                  // update the step queue for this cluster
                  stepQueue(i) = stepsRemaining
                }
              }
            }

            /* The number of completed steps is the total number of steps less the number
             * of active steps and less the number of steps still in the queue.
             */
            val nActive        = activeSteps.map(_.length).sum
            val nQueued        = stepQueue.map(_.length).sum
            val completedSteps = totalSteps - (nActive + nQueued)

            // update the progress log
            if (completedSteps > lastCompletedSteps) {
              logger.info(
                s"Job queue progress: $completedSteps/$totalSteps steps (${completedSteps * 100 / totalSteps}%)"
              )

              // update the total number of steps completed
              lastCompletedSteps = completedSteps
            }
          } catch {
            case ex: AwsServiceException if ex.isThrottlingException =>
              logger.warn("AWS rate limit exceeded, throttling...")
              Thread.sleep(2.minutes.toMillis)
          }
        }
      }

      // always terminate all clusters
      terminateClusters(clusters)

      // on a failure, throw the exception
      runResult match {
        case Success(_)  => ()
        case Failure(ex) => throw ex
      }
    }

    /** Helper: create a single job. */
    def runJob(clusterDef: ClusterDef, env: Map[String, String], job: Job): Unit = {
      runJobs(clusterDef, env, Seq(job))
    }
  }
}
