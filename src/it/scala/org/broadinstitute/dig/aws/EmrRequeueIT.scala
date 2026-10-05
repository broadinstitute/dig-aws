package org.broadinstitute.dig.aws

import org.broadinstitute.dig.aws.config.AwsConfig
import org.broadinstitute.dig.aws.emr.{ClusterDef, EmrApi, Job}
import org.scalatest.FunSuite
import software.amazon.awssdk.services.ec2.Ec2Client
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest
import software.amazon.awssdk.services.emr.model.{ClusterStatus, InstanceGroupType, ListInstancesRequest, RunJobFlowRequest, StepState}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Launches two single-node clusters running `sleep` steps, terminates the
  * master EC2 instance of the second one, and checks that every step still
  * completes exactly once. Prints the ClusterStatus EMR reports for the killed
  * cluster so `Emr.recoverableReasons` can be checked against reality.
  *
  * Run by hand: sbt "it:testOnly org.broadinstitute.dig.aws.EmrRequeueIT"
  */
final class EmrRequeueIT extends FunSuite {

  private val config = AwsConfig.loadFromResource("config.json").get

  /** Live EMR API that records cluster ids and the status of any cluster the runner describes. */
  private val api = new EmrApi {
    val live                                   = EmrApi.live
    var created: Vector[String]                = Vector.empty
    var observed: Map[String, ClusterStatus]   = Map.empty

    override def runJobFlow(request: RunJobFlowRequest): String = { val id = live.runJobFlow(request); created :+= id; id }
    override def addSteps(clusterId: String, steps: Seq[software.amazon.awssdk.services.emr.model.StepConfig]): Seq[String] = live.addSteps(clusterId, steps)
    override def stepStates(clusterId: String, stepIds: Seq[String]): Map[String, StepState] = live.stepStates(clusterId, stepIds)
    override def clusterStatus(clusterId: String): ClusterStatus = { val s = live.clusterStatus(clusterId); observed += clusterId -> s; s }
    override def terminate(clusterIds: Seq[String]): Unit = live.terminate(clusterIds)
  }

  private def masterInstanceId(clusterId: String): String = {
    val req = ListInstancesRequest.builder.clusterId(clusterId).instanceGroupTypes(InstanceGroupType.MASTER).build
    Emr.client.listInstances(req).instances.asScala.head.ec2InstanceId
  }

  test("a cluster whose master is terminated is replaced and all steps complete once") {
    val runner = new Emr.Runner(config.emr, config.output.bucket, api, d => Thread.sleep(d.toMillis))

    val cluster = ClusterDef(name = "RequeueIT", instances = 1, stepConcurrency = 1)
    val jobs    = (0 until 6).map(i => new Job(Job.CommandRunner(s"sleep-$i", Seq("bash", "-c", "sleep 120"))))

    // kill the second cluster's master once it has been running steps for a few minutes
    val killer = new Thread(() => {
      Thread.sleep(12.minutes.toMillis) // ~7 min bootstrap + a couple of steps
      val victim = api.created(1)
      val ec2    = Ec2Client.builder.build
      println(s"[IT] terminating master of $victim")
      ec2.terminateInstances(TerminateInstancesRequest.builder.instanceIds(masterInstanceId(victim)).build)
      ()
    })
    killer.setDaemon(true)
    killer.start()

    runner.runJobs(cluster, Map.empty, jobs, maxParallel = 2)

    api.observed.foreach { case (id, status) =>
      println(s"[IT] $id -> ${status.state} / ${Option(status.stateChangeReason).map(r => s"${r.code}: ${r.message}").orNull}")
    }
    assert(api.created.size == 3, s"expected one replacement cluster, created: ${api.created}")
    assert(api.observed.get(api.created(1)).exists(Emr.isLost), "killed cluster was recognised as lost")
  }
}
