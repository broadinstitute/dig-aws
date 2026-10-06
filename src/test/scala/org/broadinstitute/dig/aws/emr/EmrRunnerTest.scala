package org.broadinstitute.dig.aws.emr

import org.broadinstitute.dig.aws.Emr
import org.broadinstitute.dig.aws.config.EmrConfig
import org.broadinstitute.dig.aws.config.emr.SubnetId
import org.scalatest.FunSuite
import software.amazon.awssdk.services.emr.model.{ClusterState, ClusterStateChangeReason, ClusterStateChangeReasonCode, ClusterStatus, RunJobFlowRequest, StepState}

import scala.concurrent.duration.FiniteDuration

final class EmrRunnerTest extends FunSuite {

  private val config = EmrConfig(sshKeyName = "test-key", subnetIds = Seq(SubnetId("subnet-0123456789")))

  /** Runner wired to the fake; `createClusterRequest` is overridden so no EC2 lookup happens. */
  private def runner(fake: FakeEmr): Emr.Runner =
    new Emr.Runner(config, "test-bucket", fake, (_: FiniteDuration) => ()) {
      override protected def createClusterRequest(clusterDef: ClusterDef, env: Map[String, String]): RunJobFlowRequest =
        RunJobFlowRequest.builder.name(clusterDef.name).build
    }

  private def clusterDef(stepConcurrency: Int = 1): ClusterDef =
    ClusterDef(name = "TestStage", instances = 1, stepConcurrency = stepConcurrency)

  /** `n` independent one-step jobs, named step-0 .. step-(n-1). */
  private def jobs(n: Int): Seq[Job] =
    (0 until n).map(i => new Job(Job.CommandRunner(s"step-$i", Seq("bash", "-c", s"echo $i"))))

  test("all steps complete across clusters and every cluster is terminated") {
    val fake = new FakeEmr
    runner(fake).runJobs(clusterDef(), Map.empty, jobs(7), maxParallel = 3)

    assert(fake.clusters.size == 3)
    assert(fake.completedStepNames.sorted == (0 until 7).map(i => s"step-$i").sorted)
    assert(fake.clusters.values.forall(_.terminated))
  }

  test("a cluster lost to INSTANCE_FAILURE is replaced and its unfinished steps complete on the replacement") {
    val fake = new FakeEmr
    // Poll order: iteration 1 submits 3 steps to each cluster (no poll); iteration 2 polls
    // j-1 (poll 1) and j-2 (poll 2), moving their steps to RUNNING. Killing j-2 before
    // poll 3 (j-1's second poll) means j-1 completes normally and j-2's next poll (poll 4)
    // reports all three of its steps CANCELLED.
    fake.beforePoll = { n => if (n == 3) fake.kill("j-2", ClusterStateChangeReasonCode.INSTANCE_FAILURE) }

    runner(fake).runJobs(clusterDef(), Map.empty, jobs(6), maxParallel = 2)

    assert(fake.clusters.size == 3, "one replacement cluster was created")
    assert(fake.clusters("j-3").request.name == "TestStage", "replacement built from the same ClusterDef")
    assert(fake.completedStepNames.sorted == (0 until 6).map(i => s"step-$i").sorted, "every step completed")
    assert(fake.completedStepNames.size == 6, "no step completed twice")
    assert(fake.clusters("j-1").terminated && fake.clusters("j-3").terminated)
    assert(fake.stepsOf("j-3").nonEmpty, "lost cluster's work ran on the replacement")
  }

  test("replacementDef is applied to the replacement cluster only") {
    val fake = new FakeEmr
    fake.beforePoll = { n => if (n == 2) fake.kill("j-1", ClusterStateChangeReasonCode.INTERNAL_ERROR) }

    runner(fake).runJobs(clusterDef(), Map.empty, jobs(3), maxParallel = 1,
      replacementDef = d => d.copy(name = "TestStage_replacement"))

    assert(fake.clusters("j-1").request.name == "TestStage")
    assert(fake.clusters("j-2").request.name == "TestStage_replacement")
    assert(fake.completedStepNames.size == 3)
  }

  test("cluster lost after its active steps completed is replaced and its queue runs on the replacement") {
    val fake = new FakeEmr
    fake.beforePoll = { n =>
      if (n == 2) {
        fake.stepsOf("j-1").foreach(_.state = StepState.COMPLETED)
        fake.kill("j-1", ClusterStateChangeReasonCode.INSTANCE_FAILURE)
      }
    }

    runner(fake).runJobs(clusterDef(), Map.empty, jobs(11), maxParallel = 1)

    assert(fake.clusters.size == 2)
    assert(fake.completedStepNames.size == 11)
    assert(fake.stepsOf("j-2").size == 1)
    assert(fake.clusters("j-2").terminated)
  }

  test("a poll that reports some steps COMPLETED and others CANCELLED requeues only the unfinished ones") {
    val fake = new FakeEmr
    fake.beforePoll = { n =>
      if (n == 2) {
        fake.steps("s-1").state = StepState.COMPLETED
        fake.kill("j-1", ClusterStateChangeReasonCode.INSTANCE_FAILURE)
      }
    }

    runner(fake).runJobs(clusterDef(), Map.empty, jobs(3), maxParallel = 1)

    val doneName = fake.steps("s-1").config.name
    assert(fake.completedStepNames.size == 3)
    assert(fake.stepsOf("j-2").size == 2)
    assert(fake.stepsOf("j-2").map(_.config.name).toSet == (0 until 3).map(i => s"step-$i").toSet - doneName)
  }

  test("lost cluster with only pending steps is fully requeued") {
    val fake = new FakeEmr
    // poll 1 is j-1's first poll; its steps are all PENDING until then
    fake.beforePoll = { n => if (n == 1) fake.kill("j-1", ClusterStateChangeReasonCode.INSTANCE_FAILURE) }

    runner(fake).runJobs(clusterDef(), Map.empty, jobs(4), maxParallel = 1)

    assert(fake.clusters.size == 2)
    assert(fake.completedStepNames.sorted == (0 until 4).map(i => s"step-$i").sorted)
    assert(fake.stepsOf("j-2").size == 4, "all four steps were resubmitted")
  }

  test("two clusters lost in one cycle both get replacements") {
    val fake = new FakeEmr
    fake.beforePoll = { n =>
      if (n == 3) {
        fake.kill("j-1", ClusterStateChangeReasonCode.INSTANCE_FAILURE)
        fake.kill("j-2", ClusterStateChangeReasonCode.INTERNAL_ERROR)
      }
    }

    runner(fake).runJobs(clusterDef(), Map.empty, jobs(8), maxParallel = 2)

    assert(fake.clusters.size == 4)
    assert(fake.completedStepNames.size == 8)
    assert(fake.completedStepNames.toSet == (0 until 8).map(i => s"step-$i").toSet)
  }

  test("a replacement that is lost is replaced again within the budget") {
    val fake = new FakeEmr
    fake.beforePoll = { n =>
      if (n == 1) fake.kill("j-1", ClusterStateChangeReasonCode.INSTANCE_FAILURE)
      if (n == 2) fake.kill("j-2", ClusterStateChangeReasonCode.INSTANCE_FAILURE)
    }

    runner(fake).runJobs(clusterDef(), Map.empty, jobs(3), maxParallel = 1, maxReplacements = 2)

    assert(fake.clusters.size == 3)
    assert(fake.completedStepNames.size == 3)
  }

  test("exceeding the replacement budget terminates everything and throws") {
    val fake = new FakeEmr
    fake.beforePoll = { n => if (n <= 2) fake.kill(s"j-$n", ClusterStateChangeReasonCode.INSTANCE_FAILURE) }

    val ex = intercept[Exception] {
      runner(fake).runJobs(clusterDef(), Map.empty, jobs(3), maxParallel = 1, maxReplacements = 1)
    }

    assert(ex.getMessage.contains("replacement budget"))
    assert(fake.clusters.size == 2)
    assert(fake.clusters("j-2").terminated, "the lost cluster still in `live` was terminated by the failure path")
  }

  test("the replacement budget is shared across clusters, not per lineage") {
    val fake = new FakeEmr
    // two clusters; j-1 is lost at its first poll (poll 1) and uses the single replacement,
    // j-2 is lost at its first poll (poll 2) and finds the budget spent
    fake.beforePoll = { n =>
      if (n == 1) fake.kill("j-1", ClusterStateChangeReasonCode.INSTANCE_FAILURE)
      if (n == 2) fake.kill("j-2", ClusterStateChangeReasonCode.INSTANCE_FAILURE)
    }

    val ex = intercept[Exception] {
      runner(fake).runJobs(clusterDef(), Map.empty, jobs(4), maxParallel = 2, maxReplacements = 1)
    }

    assert(ex.getMessage.contains("replacement budget"))
    assert(fake.clusters.size == 3, "exactly one replacement was launched before the budget ran out")
  }

  test("a requeued serial job keeps its step order on the replacement") {
    val fake = new FakeEmr
    // one job of 6 serial steps; all 6 are submitted at once (maxActiveSteps = 10),
    // the cluster is lost at its first poll, and the replacement must receive them in the same order
    val serial = new Job((0 until 6).map(i => Job.CommandRunner(s"serial-$i", Seq("bash", "-c", s"echo $i"))))
    fake.beforePoll = { n => if (n == 1) fake.kill("j-1", ClusterStateChangeReasonCode.INSTANCE_FAILURE) }

    runner(fake).runJobs(clusterDef(), Map.empty, Seq(serial), maxParallel = 1)

    assert(fake.stepsOf("j-2").map(_.config.name) == (0 until 6).map(i => s"serial-$i"))
    assert(fake.completedStepNames.size == 6)
  }

  test("failed step on a healthy cluster is a genuine failure") {
    val fake = new FakeEmr
    // stepConcurrency 2 -> steps built with ActionOnFailure.CONTINUE, so the cluster stays up
    fake.beforePoll = { n => if (n == 2) fake.failStep("s-1") }

    val ex = intercept[Exception] {
      runner(fake).runJobs(clusterDef(stepConcurrency = 2), Map.empty, jobs(4), maxParallel = 1)
    }

    assert(ex.getMessage.contains("failed"))
    assert(ex.getMessage.contains("RUNNING"))
    assert(fake.clusters.size == 1, "no replacement was launched")
    assert(fake.clusters("j-1").terminated)
  }

  test("step failure that terminates its cluster (STEP_FAILURE) is a genuine failure") {
    val fake = new FakeEmr
    // stepConcurrency 1 -> TERMINATE_CLUSTER; failStep kills the cluster with STEP_FAILURE
    fake.beforePoll = { n => if (n == 2) fake.failStep("s-1") }

    val ex = intercept[Exception] {
      runner(fake).runJobs(clusterDef(), Map.empty, jobs(4), maxParallel = 2)
    }

    assert(ex.getMessage.contains("STEP_FAILURE"))
    assert(fake.clusters.size == 2, "no replacement was launched")
    assert(fake.liveClusterIds.isEmpty, "the sibling cluster was terminated too")
  }

  test("isLost accepts only terminating states with a recoverable reason") {
    def status(state: ClusterState, code: ClusterStateChangeReasonCode): ClusterStatus =
      ClusterStatus.builder.state(state).stateChangeReason(ClusterStateChangeReason.builder.code(code).build).build

    assert(Emr.isLost(status(ClusterState.TERMINATED_WITH_ERRORS, ClusterStateChangeReasonCode.INSTANCE_FAILURE)))
    assert(Emr.isLost(status(ClusterState.TERMINATING, ClusterStateChangeReasonCode.INTERNAL_ERROR)))
    assert(Emr.isLost(status(ClusterState.TERMINATED, ClusterStateChangeReasonCode.INSTANCE_FLEET_TIMEOUT)))
    assert(!Emr.isLost(status(ClusterState.TERMINATED_WITH_ERRORS, ClusterStateChangeReasonCode.STEP_FAILURE)))
    assert(!Emr.isLost(status(ClusterState.TERMINATED_WITH_ERRORS, ClusterStateChangeReasonCode.BOOTSTRAP_FAILURE)))
    assert(!Emr.isLost(status(ClusterState.TERMINATED, ClusterStateChangeReasonCode.USER_REQUEST)))
    assert(!Emr.isLost(status(ClusterState.RUNNING, ClusterStateChangeReasonCode.INSTANCE_FAILURE)))
    assert(!Emr.isLost(ClusterStatus.builder.state(ClusterState.TERMINATED_WITH_ERRORS).build), "no reason at all")
  }

  test("runJobs throws if EMR stops reporting a submitted step") {
    // a fake whose ListSteps silently omits step s-2
    val fake = new FakeEmr {
      override def stepStates(clusterId: String, stepIds: Seq[String]): Map[String, StepState] =
        super.stepStates(clusterId, stepIds) - "s-2"
    }

    val ex = intercept[Exception] {
      runner(fake).runJobs(clusterDef(), Map.empty, jobs(3), maxParallel = 1)
    }

    assert(ex.getMessage.contains("2 of 3"), s"message was: ${ex.getMessage}")
    assert(fake.liveClusterIds.isEmpty)
  }
}
