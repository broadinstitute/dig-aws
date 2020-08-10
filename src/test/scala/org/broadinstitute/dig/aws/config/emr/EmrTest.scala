package org.broadinstitute.dig.aws.config.emr

import org.broadinstitute.dig.aws.config.EmrConfig

import scala.util.Try
import org.broadinstitute.dig.aws.emr.{AmiId, ClusterDef, ReleaseLabel}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read
import org.json4s.jackson.Serialization.write
import org.scalatest.FunSuite

/**
 * Tests for org.broadinstitute.dig.aws.config.emr._
 */
final class EmrTest extends FunSuite {

  /*
   * A couple extra, custom serializers added b/c they aren't part of the
   * configuration package and aren't (typically) serialized, but they have
   * serializers just in case, so might as well test them anyway.
   */
  private val customSerializers = EmrConfig.customSerializers :+ AmiId.Serializer

  /**
   * Each case-class for the EMR cluster string types should assert if and
   * invalid string is used, which is there to make sure that a "sg-" security
   * group isn't used in place of an instance type, etc.
   */
  def testSerialize[A <: AnyRef](apply: String => A, valid: String, invalid: String*)(implicit m: Manifest[A]) = {
    implicit val formats = DefaultFormats ++ customSerializers

    // ensure the invalid string fails in the constructor
    val fails = invalid.flatMap(id => Try(apply(id)).toOption)

    // all the invalid string should have failed and returned None
    assert(fails.isEmpty)

    // ensure that the valid string passes the constructor
    val x    = apply(valid)
    val json = write(x)

    // serialize the value, make sure we get the same string (as JSON) back
    assert(json == s""""$valid"""")

    // now deserialize the JSON version of the string and get the class back
    assert(read[A](json) == x)
  }

  test("emr types - AmiId") {
    testSerialize[AmiId](AmiId.apply, "ami-123456", "amiXXX-123456")
  }

  test("emr types - ReleaseLabel") {
    testSerialize[ReleaseLabel](ReleaseLabel.apply, "emr-x.x.x", "emrXXX-x.x.x")
  }

  test("emr types - RoleId") {
    testSerialize[RoleId](RoleId.apply, "some-silly-role-id")
  }

  test("emr types - SecurityGroupId") {
    testSerialize[SecurityGroupId](SecurityGroupId.apply, "sg-123456", "sgXXX-123456")
  }

  test("emr types - SubnetId") {
    testSerialize[SubnetId](SubnetId.apply, "subnet-123456", "subnetXXX-123456")
  }

  test("emr types - Cluster name") {
    val clusterOK  = Try(ClusterDef(name = "_foo12_abc"))
    val clusterErr = Try(ClusterDef(name = "_foo12_abc bar"))

    assert(clusterOK.isSuccess)
    assert(clusterErr.isFailure)
  }
}
