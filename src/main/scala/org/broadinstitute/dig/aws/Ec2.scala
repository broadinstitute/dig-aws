package org.broadinstitute.dig.aws

import com.typesafe.scalalogging.LazyLogging

import scala.jdk.CollectionConverters._
import software.amazon.awssdk.services.ec2.Ec2Client
import software.amazon.awssdk.services.ec2.model.{DescribeInstanceTypesRequest, Filter, InstanceType, InstanceTypeInfo}

/** AWS client for creating EMR clusters and running jobs.
  */
object Ec2 extends LazyLogging {
  import MemorySize.Implicits._

  /** AWS SDK client. */
  lazy val client: Ec2Client = Ec2Client.builder.build

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
    allInstanceTypes.find(_.instanceType == it)
  }

  /** Strategy and size of instance for looking them up. */
  final case class Strategy(gen: String, vCPUs: Int, mem: MemorySize) {
    lazy val instanceType: InstanceType = {
      allInstanceTypes
        .view
        .filter(_.vCpuInfo.defaultVCpus >= vCPUs)
        .filter(_.memoryInfo.sizeInMiB >= mem.toMB.size)
        .filter(_.instanceTypeAsString.startsWith(s"$gen."))
        .toSeq
        .sortBy(_.memoryInfo.sizeInMiB)
        .headOption
        .map(_.instanceType)
        .getOrElse {
          throw new IllegalArgumentException(s"No EC2 instance type matching strategy: $toString")
        }
    }
  }

  /** Instance size strategies. */
  object Strategy {

    /** The default EC2 strategy to use. */
    val default: Strategy = generalPurpose()

    /** General purpose instances provide a balance of compute, memory and networking resources,
      * and can be used for a variety of diverse workloads. These instances are ideal for
      * applications that use these resources in equal proportions such as web servers and code
      * repositories.
      */
    def generalPurpose(vCPUs: Int = 8, mem: MemorySize = 32.gb): Strategy = {
      Strategy("m6a", vCPUs, mem)
    }

    /** Memory optimized instances are designed to deliver fast performance for workloads that
      * process large data sets in memory.
      */
    def memoryOptimized(vCPUs: Int = 8, mem: MemorySize = 64.gb): Strategy = {
      Strategy("r6a", vCPUs, mem)
    }

    /** Compute Optimized instances are ideal for compute bound applications that benefit from
      * high performance processors. Instances belonging to this family are well suited for
      * batch processing workloads, media transcoding, high performance web servers, high
      * performance computing (HPC), scientific modeling, dedicated gaming servers and ad server
      * engines, machine learning inference and other compute intensive applications.
      */
    def computeOptimized(vCPUs: Int = 8, mem: MemorySize = 16.gb): Strategy = {
      Strategy("c6a", vCPUs, mem)
    }

    /** Accelerated computing instances use hardware accelerators, or co-processors, to perform
      * functions, such as floating point number calculations, graphics processing, or data
      * pattern matching, more efficiently than is possible in software running on CPUs.
      */
    def gpuAccelerated(vCPUs: Int = 8, mem: MemorySize = 32.gb): Strategy = {
      Strategy("g4dn", vCPUs, mem)
    }
  }
}
