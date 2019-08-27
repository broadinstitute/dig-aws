package org.broadinstitute.dig.aws.config.emr

import scala.collection.Seq
import scala.reflect.ManifestFactory.classType
import scala.util.Try

import org.broadinstitute.dig.aws.emr.AmiId
import org.broadinstitute.dig.aws.emr.Cluster
import org.broadinstitute.dig.aws.emr.InstanceType
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read
import org.json4s.jackson.Serialization.write
import org.scalactic.source.Position.apply
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
  val customSerializers = EmrConfig.customSerializers ++ Seq(
    AmiId.Serializer,
    InstanceType.Serializer,
  )

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

  test("EMR types - AmiId") {
    testSerialize[AmiId](AmiId.apply, "ami-123456", "amiXXX-123456")
  }

  test("EMR types - ReleaseLabel") {
    testSerialize[ReleaseLabel](ReleaseLabel.apply, "emr-x.x.x", "emrXXX-x.x.x")
  }

  test("EMR types - RoleId") {
    testSerialize[RoleId](RoleId.apply, "some-silly-role-id")
  }

  test("EMR types - SecurityGroupId") {
    testSerialize[SecurityGroupId](SecurityGroupId.apply, "sg-123456", "sgXXX-123456")
  }

  test("EMR types - SubnetId") {
    testSerialize[SubnetId](SubnetId.apply, "subnet-123456", "subnetXXX-123456")
  }

  test("EMR types - InstanceType") {
    val someKnownInstanceTypes = List(
      InstanceType.m5_2xlarge,
      InstanceType.c5_9xlarge,
    )

    someKnownInstanceTypes.foreach { it =>
      testSerialize(InstanceType.apply, it.value)
    }
  }

  test("EMR types - Cluster name") {
    val clusterOK  = Try(Cluster(name = "_foo12_abc"))
    val clusterErr = Try(Cluster(name = "_foo12_abc bar"))

    assert(clusterOK.isSuccess)
    assert(clusterErr.isFailure)
  }
}
