package org.broadinstitute.dig.aws.emr

import org.broadinstitute.dig.aws.Emr
import org.broadinstitute.dig.aws.Utils.awsRetry
import software.amazon.awssdk.services.emr.EmrClient
import software.amazon.awssdk.services.emr.model.{Unit => _, _}

import scala.jdk.CollectionConverters._

/** The subset of the EMR API used by `Emr.Runner`, so it can be faked in tests.
  * Every call in the live implementation goes through `awsRetry`, which
  * retries throttling and other retryable SDK exceptions with backoff.
  */
trait EmrApi {

  /** Create a cluster; returns its job flow id. */
  def runJobFlow(request: RunJobFlowRequest): String

  /** Add steps to a cluster; returns their ids in the same order. */
  def addSteps(clusterId: String, steps: Seq[StepConfig]): Seq[String]

  /** Current state of the given steps. */
  def stepStates(clusterId: String, stepIds: Seq[String]): Map[String, StepState]

  /** Current status (state + state change reason) of a cluster. */
  def clusterStatus(clusterId: String): ClusterStatus

  /** Terminate clusters. */
  def terminate(clusterIds: Seq[String]): Unit
}

object EmrApi {

  /** Live implementation backed by the AWS SDK client. */
  final class Live(client: EmrClient) extends EmrApi {

    override def runJobFlow(request: RunJobFlowRequest): String = {
      awsRetry()(client.runJobFlow(request).jobFlowId)
    }

    override def addSteps(clusterId: String, steps: Seq[StepConfig]): Seq[String] = {
      val req = AddJobFlowStepsRequest.builder.jobFlowId(clusterId).steps(steps.asJava).build
      awsRetry()(client.addJobFlowSteps(req).stepIds.asScala.toSeq)
    }

    override def stepStates(clusterId: String, stepIds: Seq[String]): Map[String, StepState] = {
      // AWS ListSteps API has a limit of 10 step IDs per request, so batch the requests
      stepIds.grouped(10).flatMap { batch =>
        val req = ListStepsRequest.builder.clusterId(clusterId).stepIds(batch.asJava).build
        awsRetry()(client.listStepsPaginator(req).steps.asScala.toList).map(s => s.id -> s.status.state)
      }.toMap
    }

    override def clusterStatus(clusterId: String): ClusterStatus = {
      val req = DescribeClusterRequest.builder.clusterId(clusterId).build
      awsRetry()(client.describeCluster(req).cluster.status)
    }

    override def terminate(clusterIds: Seq[String]): Unit = {
      clusterIds.grouped(10).foreach { ids =>
        val req = TerminateJobFlowsRequest.builder.jobFlowIds(ids.asJava).build
        awsRetry()(client.terminateJobFlows(req))
        ()
      }
    }
  }

  /** Shared live API; all runners can share a single client. */
  lazy val live: EmrApi = new Live(Emr.client)
}
