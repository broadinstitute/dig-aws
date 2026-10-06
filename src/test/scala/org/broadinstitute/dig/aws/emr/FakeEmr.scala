package org.broadinstitute.dig.aws.emr

import software.amazon.awssdk.services.emr.model.{Unit => _, _}

import scala.collection.mutable

/** In-memory stand-in for EMR.
  *
  * Every `stepStates` poll advances each step of a live cluster one state:
  * PENDING -> RUNNING -> COMPLETED. `kill` marks a cluster dead with a reason
  * code: its unfinished steps become CANCELLED and `clusterStatus` reports
  * TERMINATED_WITH_ERRORS. `failStep` fails one step; if that step was built
  * with TERMINATE_CLUSTER the cluster dies with STEP_FAILURE, as EMR does.
  */
class FakeEmr extends EmrApi { // not final: Task 5 subclasses it
  final class FakeStep(val cluster: String, val config: StepConfig, var state: StepState)
  final class FakeCluster(val id: String, val request: RunJobFlowRequest) {
    var dead: Option[ClusterStateChangeReasonCode] = None
    var terminated: Boolean                        = false
  }

  val clusters: mutable.LinkedHashMap[String, FakeCluster] = mutable.LinkedHashMap.empty
  val steps: mutable.LinkedHashMap[String, FakeStep]       = mutable.LinkedHashMap.empty

  /** Number of `stepStates` calls so far; `beforePoll` sees the new count. */
  var polls: Int                 = 0
  var beforePoll: Int => Unit    = _ => ()

  def kill(clusterId: String, reason: ClusterStateChangeReasonCode): Unit = {
    clusters(clusterId).dead = Some(reason)
    steps.values
      .filter(s => s.cluster == clusterId && s.state != StepState.COMPLETED)
      .foreach(_.state = StepState.CANCELLED)
  }

  def failStep(stepId: String): Unit = {
    val step = steps(stepId)
    step.state = StepState.FAILED
    if (step.config.actionOnFailure == ActionOnFailure.TERMINATE_CLUSTER) {
      kill(step.cluster, ClusterStateChangeReasonCode.STEP_FAILURE)
    }
  }

  def stepsOf(clusterId: String): Seq[FakeStep] = steps.values.filter(_.cluster == clusterId).toSeq
  def completedStepNames: Seq[String]           = steps.values.filter(_.state == StepState.COMPLETED).map(_.config.name).toSeq
  def liveClusterIds: Seq[String]               = clusters.values.filter(c => c.dead.isEmpty && !c.terminated).map(_.id).toSeq

  override def runJobFlow(request: RunJobFlowRequest): String = {
    val id = s"j-${clusters.size + 1}"
    clusters(id) = new FakeCluster(id, request)
    id
  }

  override def addSteps(clusterId: String, configs: Seq[StepConfig]): Seq[String] = {
    require(clusters(clusterId).dead.isEmpty, s"AddJobFlowSteps on dead cluster $clusterId")
    configs.map { config =>
      val id = s"s-${steps.size + 1}"
      steps(id) = new FakeStep(clusterId, config, StepState.PENDING)
      id
    }
  }

  override def stepStates(clusterId: String, stepIds: Seq[String]): Map[String, StepState] = {
    polls += 1
    beforePoll(polls)

    stepIds.map { id =>
      val step = steps(id)
      if (clusters(clusterId).dead.isEmpty) {
        step.state = step.state match {
          case StepState.PENDING => StepState.RUNNING
          case StepState.RUNNING => StepState.COMPLETED
          case other             => other
        }
      }
      id -> step.state
    }.toMap
  }

  override def clusterStatus(clusterId: String): ClusterStatus = {
    val cluster = clusters(clusterId)
    cluster.dead match {
      case Some(reason) =>
        ClusterStatus.builder
          .state(ClusterState.TERMINATED_WITH_ERRORS)
          .stateChangeReason(ClusterStateChangeReason.builder.code(reason).build)
          .build
      case None if cluster.terminated =>
        ClusterStatus.builder
          .state(ClusterState.TERMINATED)
          .stateChangeReason(ClusterStateChangeReason.builder.code(ClusterStateChangeReasonCode.USER_REQUEST).build)
          .build
      case None =>
        ClusterStatus.builder.state(ClusterState.RUNNING).build
    }
  }

  override def terminate(clusterIds: Seq[String]): Unit = {
    clusterIds.foreach(id => clusters(id).terminated = true)
  }
}
