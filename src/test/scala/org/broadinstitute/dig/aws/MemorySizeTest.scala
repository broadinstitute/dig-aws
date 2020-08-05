package org.broadinstitute.dig.aws

import org.scalatest.FunSuite

final class MemorySizeTest extends FunSuite {
  import org.broadinstitute.dig.aws.MemorySize.Implicits._

  test("implicit conversion") {
    assert(10.b.units == B)
    assert(10.mb.units == MB)
    assert(10.gb.units == GB)
  }

  test("memory size conversion") {
    assert(8.b.size == 8)
    assert(8.mb.toB.size == 8 * 1024 * 1024)
    assert(8.gb.toMB.size == 8 * 1024)
    assert(8.gb.toB.size == 8 * 1024 * 1024 * 1024)
  }

  test("memory size strings") {
    assert(8.mb.toString == "8m")
    assert(8.gb.toString == "8g")
    assert(8.b.toString == "8b")
  }
}
