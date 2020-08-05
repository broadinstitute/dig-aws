package org.broadinstitute.dig.aws

import org.scalatest.FunSuite

final class Ec2Test extends FunSuite {
  import org.broadinstitute.dig.aws.Ec2.Strategy

  test("fetch instance types") {
    assert(Ec2.allInstanceTypes.nonEmpty)
    assert(Ec2.allInstanceTypes.size > 12)
  }

  test("find instance types by size") {
    val sizes = Seq(
      Strategy.generalPurpose(),
      Strategy.memoryOptimized(),
      Strategy.computeOptimized(),
      Strategy.gpuAccelerated(),
    )

    // ensure that all sizes can be found
    sizes.foreach(_.instanceType)
  }
}
