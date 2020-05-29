package org.broadinstitute.dig.aws

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dig.aws.ec2.InstanceType
import org.broadinstitute.dig.aws.ec2.InstanceType.Strategy
import software.amazon.awssdk.services.ec2.Ec2Client
import software.amazon.awssdk.services.ec2.model.{DescribeInstanceTypesRequest, Filter, InstanceTypeInfo}

import scala.jdk.CollectionConverters._

/** AWS client for creating EMR clusters and running jobs.
  */
object Ec2 extends LazyLogging {
  import MemorySize.Implicits._

  /** AWS SDK client. All runners can share a single client. */
  val client: Ec2Client = Ec2Client.builder.build

  /** Fetch all current gen instances large enough for EMR usage. */
  lazy val allInstanceTypes: Seq[InstanceTypeInfo] = {
    val currentGen = Filter.builder.name("current-generation").values("true").build
    val req = DescribeInstanceTypesRequest.builder.filters(currentGen).build

    client.describeInstanceTypesPaginator(req)
      .iterator.asScala
      .flatMap(_.instanceTypes.asScala)
      .filter(_.vCpuInfo.defaultVCpus >= 4)
      .filter(_.memoryInfo.sizeInMiB >= 16.gb.toMB.size)
      .filter(it => "mrcg".contains(it.instanceTypeAsString.head))
      .toSeq
  }

  /** Lookup the instance type info for a given instance type. */
  def instanceTypeInfo(it: InstanceType): Option[InstanceTypeInfo] = {
    allInstanceTypes.find(_.instanceTypeAsString == it.value)
  }

  /** Validate an instance type string. */
  def isValidInstanceType(it: InstanceType): Boolean = {
    instanceTypeInfo(it).isDefined
  }

  /** Returns the smallest instance type that meets the requirements. */
  def instanceTypeWithStrategy(strategy: Strategy, vCPUs: Int=4, mem: MemorySize=32.gb): Option[InstanceType] = {
    allInstanceTypes
      .view
      .filter(_.vCpuInfo.defaultVCpus >= vCPUs)
      .filter(_.memoryInfo.sizeInMiB >= mem.toMB.size)
      .filter(_.instanceTypeAsString.startsWith(strategy.prefix))
      .toSeq
      .sortBy(_.memoryInfo.sizeInMiB)
      .headOption
      .map(it => InstanceType(it.instanceTypeAsString))
  }
}
