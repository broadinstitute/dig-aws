package org.broadinstitute.dig.aws.emr

import org.broadinstitute.dig.aws.Emr
import org.broadinstitute.dig.aws.config.EmrConfig
import org.broadinstitute.dig.aws.config.emr.SubnetId
import org.scalatest.FunSuite
import software.amazon.awssdk.services.emr.model.{ClusterStateChangeReasonCode, RunJobFlowRequest, StepState}

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
}
