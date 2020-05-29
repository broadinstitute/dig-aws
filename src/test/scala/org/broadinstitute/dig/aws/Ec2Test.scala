package org.broadinstitute.dig.aws

import org.scalatest.FunSuite

final class Ec2Test extends FunSuite {
  import org.broadinstitute.dig.aws.ec2.InstanceType
  import org.broadinstitute.dig.aws.MemorySize.Implicits._

  test("fetch instance types") {
    assert(Ec2.allInstanceTypes.nonEmpty)
  }

  test("instance type validity checks") {
    assert(Ec2.isValidInstanceType(InstanceType.default))
  }

  test("find instance types") {
    val instanceTypes = Seq(
      Ec2.instanceTypeWithStrategy(InstanceType.GeneralPurpose, vCPUs=8, mem=32.gb),
      Ec2.instanceTypeWithStrategy(InstanceType.MemoryOptimized, mem=72.gb),
      Ec2.instanceTypeWithStrategy(InstanceType.ComputeOptimized, vCPUs=16),
      Ec2.instanceTypeWithStrategy(InstanceType.GpuAccelerated, mem=32.gb),
    )

    // get the info of each instance
    val instanceInfo = instanceTypes.map(_.flatMap(Ec2.instanceTypeInfo))

    // ensure requirements were met
    val ok = instanceTypes.zip(instanceInfo).forall {
      case (Some(it), Some(info)) => true
      case _                      => false
    }

    assert(ok)
  }
}
