package org.broadinstitute.dig.aws.config.emr

import org.scalatest.FunSuite

/**
  * @author clint
  * @date Oct 6, 2021
  */
final class EbsVolumeTypeTest extends FunSuite {
  import EbsVolumeType._

  test("value") {
    assert(Gp2.value === "gp2")
    assert(Io1.value === "io1")
    assert(Standard.value === "standard")
  }

  test("values") {
    assert(EbsVolumeType.values.toSet === Set(Gp2, Io1, Standard))
  }

  import org.json4s._

  test("serialize") {
    import EbsVolumeType.JsonSupport.serialize

    assert(serialize(Gp2) === JString("gp2"))
    assert(serialize(Io1) === JString("io1"))
    assert(serialize(Standard) === JString("standard"))
  }

  test("deserialize") {
    import EbsVolumeType.JsonSupport.deserialize

    assert(deserialize(JString("gp2")) === Gp2)
    assert(deserialize(JString("io1")) === Io1)
    assert(deserialize(JString("standard")) === Standard)

    assert(deserialize.isDefinedAt(JObject()) === false)
    assert(deserialize.isDefinedAt(JInt(42)) === false)
    assert(deserialize.isDefinedAt(JNull) === false)
  }
}
