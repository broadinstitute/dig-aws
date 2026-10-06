package org.broadinstitute.dig.aws

import org.broadinstitute.dig.aws.emr.{ClusterDef, EmrApi, Job}
import org.scalatest.FunSuite
import software.amazon.awssdk.services.ec2.Ec2Client
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest
import software.amazon.awssdk.services.emr.model.{ClusterState, ClusterStatus, InstanceGroupType, ListInstancesRequest, RunJobFlowRequest, StepConfig, StepState}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Launches two single-node clusters running `sleep` steps, waits for the
  * second one to reach RUNNING (plus a minute of step progress), then terminates
  * its master EC2 instance and checks that the runner replaces it and finishes.
  * Prints the ClusterStatus EMR reports for each observed cluster (even if
  * `runJobs` throws) so `Emr.recoverableReasons` can be checked against the
  * printed reason.
  *
  * Run by hand: sbt "it:testOnly org.broadinstitute.dig.aws.EmrRequeueIT"
  */
final class EmrRequeueIT extends FunSuite {

  private val config = ItConfig.load()

  /** Live EMR API that records cluster ids and the status of any cluster the runner describes. */
  private final class RecordingApi(live: EmrApi) extends EmrApi {
    @volatile var created: Vector[String]              = Vector.empty
    @volatile var observed: Map[String, ClusterStatus] = Map.empty

    override def runJobFlow(request: RunJobFlowRequest): String = { val id = live.runJobFlow(request); created :+= id; id }
    override def addSteps(clusterId: String, steps: Seq[StepConfig]): Seq[String] = live.addSteps(clusterId, steps)
    override def stepStates(clusterId: String, stepIds: Seq[String]): Map[String, StepState] = live.stepStates(clusterId, stepIds)
    override def clusterStatus(clusterId: String): ClusterStatus = { val s = live.clusterStatus(clusterId); observed += clusterId -> s; s }
    override def terminate(clusterIds: Seq[String]): Unit = live.terminate(clusterIds)
  }

  private val api = new RecordingApi(EmrApi.live)

  private def masterInstanceId(clusterId: String): String = {
    val req = ListInstancesRequest.builder.clusterId(clusterId).instanceGroupTypes(InstanceGroupType.MASTER).build
    Emr.client.listInstances(req).instances.asScala.head.ec2InstanceId
  }

  /** Block until the second cluster exists and is RUNNING; give up after 20 minutes. */
  private def awaitVictimRunning(): String = {
    val deadline = System.currentTimeMillis + 20.minutes.toMillis
    while (true) {
      if (System.currentTimeMillis > deadline) throw new RuntimeException("second cluster never reached RUNNING")
      api.created.lift(1) match {
        case Some(victim) if EmrApi.live.clusterStatus(victim).state == ClusterState.RUNNING => return victim
        case _ => Thread.sleep(30.seconds.toMillis)
      }
    }
    throw new IllegalStateException("unreachable")
  }

  test("a cluster whose master is terminated is replaced and all steps complete once") {
    val runner = new Emr.Runner(config.emr, config.output.bucket, api, d => Thread.sleep(d.toMillis))

    val cluster = ClusterDef(name = "RequeueIT", instances = 1, stepConcurrency = 1)
    val jobs    = (0 until 8).map(i => new Job(Job.CommandRunner(s"sleep-$i", Seq("bash", "-c", "sleep 120"))))

    // kill the second cluster's master once it is running and has started on its steps
    val killer = new Thread(() => {
      try {
        val victim = awaitVictimRunning()
        Thread.sleep(60.seconds.toMillis)
        val ec2 = Ec2Client.builder.build
        println(s"[IT] terminating master of $victim")
        ec2.terminateInstances(TerminateInstancesRequest.builder.instanceIds(masterInstanceId(victim)).build)
        ()
      } catch {
        case e: Throwable => println(s"[IT] killer failed: $e"); e.printStackTrace()
      }
    })
    killer.setDaemon(true)
    killer.start()

    try {
      runner.runJobs(cluster, Map.empty, jobs, maxParallel = 2)
    } finally {
      api.observed.foreach { case (id, status) =>
        println(s"[IT] $id -> ${status.state} / ${Option(status.stateChangeReason).map(r => s"${r.code}: ${r.message}").orNull}")
      }
    }
    assert(api.created.size == 3, s"expected one replacement cluster, created: ${api.created}")
    assert(api.observed.get(api.created(1)).exists(Emr.isLost), "killed cluster was recognised as lost")
  }
}
