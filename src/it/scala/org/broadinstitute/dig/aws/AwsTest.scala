package org.broadinstitute.dig.aws

import org.broadinstitute.dig.aws.config.AwsConfig

final class AwsTest extends AwsFunSuite {
  import Implicits._

  private val config = AwsConfig.loadFromResource("config.json").get

  override val s3: S3.Bucket   = new S3.Bucket(config.s3.bucket)
  override val emr: Emr.Runner = new Emr.Runner(config.emr, config.s3.bucket)

  /**
    * Create a cluster and run a simple script job.
    */
  testWithCluster("Simple Cluster", "test_script.py")

  /**
    * Create 1 non-pseudo-dir object and list it
    */
  testWithPseudoDirIO("PutLsNonDir") {
    doPutLsOneObjectTest(_ + "/foo")
  }

  /**
    * Create 1 pseudo-dir object and list it
    */
  testWithPseudoDirIO("PutLsDir") {
    doPutLsOneObjectTest(_ + "/foo/")
  }

  /**
    * Create 1 object inside a pseudo-dir and list it
    */
  testWithPseudoDirIO("PutLs1") {
    doPutLsTest(1)
  }

  /**
    * Create 10 objects and list them
    */
  testWithPseudoDirIO("PutLs10") {
    doPutLsTest(10)
  }

  /**
    * Create 2500 objects and list them
    *
    * NOTE: We do this many so we can test aws.ls and aws.rm looping/recursion
    *       with the API returning only allowing a maximum number of keys to
    *       be returned/acted upon at once.
    */
  testWithPseudoDirIO("PutLs2500") {
    doPutLsTest(2500)
  }

  testWithPseudoDirIO("Upload") {
    doUploadTest("test_upload.txt")
  }

  // Upload a resource file
  private def doUploadTest(resource: String): String => Unit = { pseudoDirKey =>
    val key = s"$pseudoDirKey/${resource.stripPrefix("/")}"

    assert(s3.ls(key).isEmpty)
    s3.putResource(key, resource)
    assert(s3.ls(key).nonEmpty)
    s3.rm(key)
    assert(s3.ls(key).isEmpty)
    ()
  }

  //Create one object and list it
  private def doPutLsOneObjectTest(makeKey: String => String): String => Unit = { pseudoDirKey =>
    val key     = makeKey(pseudoDirKey)
    val content = "ABC"

    assert(s3.ls(key).isEmpty)
    s3.put(key, content)
    assert(s3.ls(key).nonEmpty)
    assert(s3.get(key).mkString() == content)

    // remove it
    s3.rm(key)
    assert(s3.ls(key).isEmpty)
    ()
  }

  //Create n objects, then list them
  private def doPutLsTest(n: Int): String => Unit = { pseudoDirKey =>
    val pseudoDirKeyWithSlash = s"$pseudoDirKey/"
    val expectedKeys          = (1 to n).toList.map(i => s"$pseudoDirKey/$i")

    assert(s3.ls(pseudoDirKeyWithSlash).isEmpty)

    expectedKeys.zipWithIndex.foreach {
      case (k, i) =>
        s3.put(k, i.toString)
    }

    val putKeys = s3.ls(pseudoDirKeyWithSlash).map(_.key)

    // ensure they are all there
    assert(expectedKeys.forall(putKeys.contains))

    // remove them all
    s3.rm(pseudoDirKeyWithSlash)
    assert(s3.ls(pseudoDirKeyWithSlash).isEmpty)
    ()
  }
}
