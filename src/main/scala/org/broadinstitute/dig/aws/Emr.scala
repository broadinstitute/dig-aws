package org.broadinstitute.dig.aws

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dig.aws.config.EmrConfig
import org.broadinstitute.dig.aws.emr.{ClusterDef, EmrApi, Job}
import org.broadinstitute.dig.aws.emr.configurations.Configuration

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Random, Success, Try}
import software.amazon.awssdk.services.emr.EmrClient
import software.amazon.awssdk.services.emr.model.{ClusterState, ClusterStateChangeReasonCode, ClusterStatus, JobFlowInstancesConfig, RunJobFlowRequest, StepConfig, StepState}

import scala.collection.immutable.VectorMap
import scala.collection.mutable

/** AWS client for creating EMR clusters and running jobs.
  */
object Emr extends LazyLogging {

  /** AWS SDK client. All runners can share a single client. */
  lazy val client: EmrClient = EmrClient.builder.build

  /** Cluster state change reasons that mean the cluster was lost through no
    * fault of its steps (Spot reclaim, hardware failure, EMR internal error).
    * Any other reason is treated as a genuine failure of the work.
    */
  val recoverableReasons: Set[ClusterStateChangeReasonCode] = Set(
    ClusterStateChangeReasonCode.INSTANCE_FAILURE,
    ClusterStateChangeReasonCode.INTERNAL_ERROR,
    ClusterStateChangeReasonCode.INSTANCE_FLEET_TIMEOUT,
  )

  /** True if the cluster is going away for a recoverable reason. */
  def isLost(status: ClusterStatus): Boolean = {
    val goingAway = status.state match {
      case ClusterState.TERMINATING | ClusterState.TERMINATED | ClusterState.TERMINATED_WITH_ERRORS => true
      case _                                                                                       => false
    }

    goingAway && reasonCode(status).exists(recoverableReasons.contains)
  }

  /** The reason code EMR gave for the cluster's last state change, if any. */
  private def reasonCode(status: ClusterStatus): Option[ClusterStateChangeReasonCode] =
    Option(status.stateChangeReason).map(_.code)

  /** Runners launch and add steps to job clusters. */
  class Runner(config: EmrConfig, logBucket: String, api: EmrApi, sleep: FiniteDuration => Unit) {

    /** Production constructor: live EMR API and real sleeping. */
    def this(config: EmrConfig, logBucket: String) = {
      this(config, logBucket, EmrApi.live, (d: FiniteDuration) => Thread.sleep(d.toMillis))
    }

    private val subnetIterator = Iterator.continually(config.subnetIds).flatten

    /** Build the request that creates a cluster for `clusterDef`. Protected so
      * tests can override it (ClusterDef.instanceGroups looks up EC2 instance types).
      */
    protected def createClusterRequest(clusterDef: ClusterDef, env: Map[String, String]): RunJobFlowRequest = {
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
      request
    }

    private def createCluster(clusterDef: ClusterDef, env: Map[String, String]): String = {
      api.runJobFlow(createClusterRequest(clusterDef, env))
    }

    /** Terminate a list of running clusters. */
    private def terminateClusters(clusterIds: Seq[String]): Unit = {
      api.terminate(clusterIds)
      logger.info("Clusters terminated.")
    }

    /** A cluster's share of the work: steps not yet submitted and steps in flight. */
    private final class ClusterRun(
      var id: String,
      var queue: List[() => StepConfig],
      var active: VectorMap[String, () => StepConfig],
    )

    /** Runs jobs across multiple clusters, terminating clusters when their work is complete.
      *
      * A cluster that dies for a recoverable reason (see `Emr.isLost`) is replaced
      * by a new cluster built from `replacementDef(clusterDef)` and its unfinished
      * steps are resubmitted there, up to `maxReplacements` times per call. A step
      * failure, or any other cluster failure, terminates every cluster and throws.
      *
      * `replacementDef` must not change `stepConcurrency` or `bootstrapSteps`: the
      * steps' action-on-failure was decided from the original definition.
      */
    def runJobs(
      clusterDef: ClusterDef,
      env: Map[String, String],
      jobs: Seq[Job],
      maxParallel: Int = 5,
      maxReplacements: Int = 5,
      replacementDef: ClusterDef => ClusterDef = identity,
    ): Unit = {
      val allJobs = jobs.flatMap {
        case job if job.parallelSteps => job.steps.map(new Job(_))
        case job                      => Seq(job)
      }
      val maxActiveSteps     = 10
      val nClusters          = allJobs.size.min(maxParallel)
      val totalSteps         = jobs.flatMap(_.steps).size
      val terminateOnFailure = clusterDef.stepConcurrency == 1 || clusterDef.bootstrapSteps.nonEmpty

      logger.info(s"Creating $nClusters clusters for ${jobs.size} jobs...")

      // Shuffle jobs once, then deal them round-robin across clusters. The build
      // is deferred so a step can be rebuilt if it has to move to a replacement.
      val shuffledJobs = Random.shuffle(allJobs)
      val runs: Vector[ClusterRun] = (0 until nClusters).toVector.map { i =>
        sleep(1.second) // delay to avoid rate limiting
        val id    = createCluster(clusterDef, env)
        val steps = shuffledJobs.zipWithIndex.collect { case (job, idx) if idx % nClusters == i => job }
        new ClusterRun(id, steps.flatMap(_.steps).map(step => () => step.build(terminateOnFailure)).toList, VectorMap.empty)
      }
      logger.info("Clusters launched.")

      val live             = mutable.ListBuffer(runs: _*)
      var completedSteps   = 0
      var replacementsUsed = 0
      var lastReported     = -1

      def replace(run: ClusterRun, reason: String): Unit = {
        if (replacementsUsed >= maxReplacements) {
          throw new Exception(s"Cluster ${run.id} lost ($reason) and the replacement budget of $maxReplacements is spent")
        }
        replacementsUsed += 1

        val unfinished = run.active.values.toList ++ run.queue
        logger.warn(s"Cluster ${run.id} lost ($reason); launching replacement ${replacementsUsed}/$maxReplacements for ${unfinished.size} steps")

        run.id     = createCluster(replacementDef(clusterDef), env)
        run.queue  = unfinished
        run.active = VectorMap.empty
      }

      def lostReason(status: ClusterStatus): String =
        s"${status.state}/${reasonCode(status).orNull}"

      def reasonMessage(status: ClusterStatus): String =
        Option(status.stateChangeReason).flatMap(r => Option(r.message)).map(m => s": $m").getOrElse("")

      // submit up to the concurrency limit from the cluster's queue
      def topUp(run: ClusterRun): Unit = {
        val (toAdd, remaining) = run.queue.splitAt(maxActiveSteps - run.active.size)
        val configs            = toAdd.map(build => build())
        val stepIds            = api.addSteps(run.id, configs)
        logger.info(s"Submitted ${configs.size} steps to ${run.id}: ${configs.map(_.name).mkString(", ")}")
        run.active = run.active ++ stepIds.zip(toAdd)
        run.queue  = remaining
      }

      val runResult = Try {
        while (live.nonEmpty) {
          live.toList.foreach { run =>
            // poll the steps in flight
            if (run.active.nonEmpty) {
              val states    = api.stepStates(run.id, run.active.keys.toSeq)
              val completed = states.collect { case (id, StepState.COMPLETED) => id }.toSet
              val bad       = states.collect { case (id, StepState.FAILED | StepState.CANCELLED | StepState.INTERRUPTED) => id }

              // steps that finished in this poll are done whatever happens to the cluster next
              completedSteps += completed.size
              run.active = run.active -- completed

              if (bad.nonEmpty) {
                val status = api.clusterStatus(run.id)
                if (Emr.isLost(status)) {
                  replace(run, lostReason(status))
                } else {
                  throw new Exception(s"${run.id} failed: steps ${bad.mkString(", ")} ${states(bad.head)}; cluster ${lostReason(status)}${reasonMessage(status)}")
                }
              } else {
                // keep only steps EMR still reports as in flight (PENDING, RUNNING, CANCEL_PENDING);
                // dropped here: ids EMR no longer reports, and any state that is neither in
                // flight, completed nor bad. The completion check at the end catches those.
                val inFlight = Set(StepState.PENDING, StepState.RUNNING, StepState.CANCEL_PENDING)
                run.active = VectorMap.from(run.active.filter { case (id, _) => states.get(id).exists(inFlight.contains) })
              }
            }

            // top up to the step concurrency limit; a cluster that died since the last
            // poll rejects the submission, in which case it is replaced like any lost cluster
            if (run.active.size < maxActiveSteps && run.queue.nonEmpty) {
              Try(topUp(run)) match {
                case Success(_) => ()
                case Failure(ex) =>
                  val status = api.clusterStatus(run.id)
                  if (Emr.isLost(status)) {
                    replace(run, lostReason(status))
                    topUp(run)
                  } else {
                    throw new Exception(s"${run.id} rejected new steps; cluster ${lostReason(status)}${reasonMessage(status)}", ex)
                  }
              }
            }

            // nothing left for this cluster
            if (run.queue.isEmpty && run.active.isEmpty) {
              logger.info(s"Terminating cluster ${run.id} because all assigned work has been distributed and completed.")
              api.terminate(Seq(run.id))
              live -= run
            }
            sleep(5.seconds)
          }

          if (completedSteps > lastReported) {
            logger.info(s"Global job progress: $completedSteps/$totalSteps steps (${completedSteps * 100 / totalSteps}%)")
            lastReported = completedSteps
          }
        }

        logger.info(s"Replacements used: $replacementsUsed/$maxReplacements")
        if (completedSteps != totalSteps) {
          throw new Exception(s"Only $completedSteps of $totalSteps steps were reported COMPLETED")
        }
      }

      // Ensure all clusters are properly terminated, especially on exceptions
      runResult match {
        case Failure(ex) =>
          logger.error(s"An exception occurred during job execution: ${ex.getMessage}. Terminating all remaining clusters.")
          terminateClusters(live.map(_.id).toList)
          throw ex
        case Success(_) =>
          logger.info("All clusters have terminated their work.")
      }
    }

    def runJob(clusterDef: ClusterDef, env: Map[String, String], job: Job): Unit = {
      runJobs(clusterDef, env, Seq(job))
    }
  }
}
