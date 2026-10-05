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
}
