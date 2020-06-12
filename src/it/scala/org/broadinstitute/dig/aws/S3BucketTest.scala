package org.broadinstitute.dig.aws

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant

import scala.io.Source
import org.broadinstitute.dig.aws.config.{AwsConfig, EmrConfig}
import org.json4s.DefaultFormats
import org.json4s.Formats
import org.json4s.jackson.Serialization.read


/**
  * @author clint
  * Jul 27, 2018
  */
final class S3BucketTest extends AwsFunSuite {
  
  /**
    * Put() an object, then get() it
    */
  testWithPseudoDir("PutGet") {
    doPutGetTest
  }
  
  /**
		* Put() a file, then get() it
		*/
  testWithPseudoDir("PutGetFile") {
    doPutGetFileTest
  }

  /**
    * Make a pseudo-dir, then make the same one again with different metadata
    */
  testWithPseudoDir("Mkdir") {
    doMkdirTest
  }

  /**
    * Create 1 non-pseudo-dir object and list it
    */
  testWithPseudoDir("PutLsNonDir") {
    doPutLsOneObjectTest(_ + "/foo")
  }

  /**
    * Create 1 pseudo-dir object and list it
    */
  testWithPseudoDir("PutLsDir") {
    doPutLsOneObjectTest(_ + "/foo/")
  }

  /**
    * Create 1 object inside a pseudo-dir and list it
    */
  testWithPseudoDir("PutLs1") {
    doPutLsTest(1)
  }

  /**
    * Create 10 objects and list them
    */
  testWithPseudoDir("PutLs10") {
    doPutLsTest(10)
  }

  /**
    * Create 2500 objects and list them
    */
  testWithPseudoDir("PutLs2500") {
    doPutLsTest(2500)
  }

  /**
    * Create an object and delete it
    */
  testWithPseudoDir("RmExists") {
    doRmTest
  }

  /**
    * Create 10 objects in a pseudo-dir and delete them
    */
  testWithPseudoDir("RmDir10") {
    doRmDirTest(10)
  }

  /**
    * Create 2500 objects in a pseudo-dir and delete them
    */
  testWithPseudoDir("RmDir2500") {
    doRmDirTest(2500)
  }

  testWithPseudoDir("Upload") {
    doUploadTest("test_upload.txt")
  }

  // Upload a resource file
  private def doUploadTest(resource: String): String => IO[Unit] = { pseudoDirKey =>
    val key = s"${pseudoDirKey}/${resource.stripPrefix("/")}"

    for {
      shouldntExist <- aws.ls(key)
      uri           <- aws.upload(resource, pseudoDirKey)
      shouldExist   <- aws.ls(key)
    } yield {
      assert(shouldntExist === Nil)
      assert(uri === aws.uriOf(key))
      assert(shouldExist === Seq(key))
      ()
    }
  }
  
  //Create one object and list it
  private def doPutLsOneObjectTest(makeKey: String => String): String => IO[Unit] = { pseudoDirKey =>
    val key = makeKey(pseudoDirKey)

    for {
      shouldntExist <- aws.ls(key)
      _             <- aws.put(key, "abc")
      shouldExist   <- aws.ls(key)
    } yield {
      assert(shouldntExist == Nil)
      assert(shouldExist == Seq(key))
      ()
    }
  }

  //Create n objects, then list them
  private def doPutLsTest(n: Int): String => IO[Unit] = { pseudoDirKey =>
    import cats.implicits._

    def toKey(i: Int) = s"${pseudoDirKey}/${i}"

    val pseudoDirKeyWithSlash = s"$pseudoDirKey/"

    for {
      keysBeforePut <- aws.ls(pseudoDirKeyWithSlash)
      _             <- (1 to n).toList.map(i => aws.put(toKey(i), i.toString)).sequence
      keys          <- aws.ls(pseudoDirKeyWithSlash)
    } yield {
      assert(keysBeforePut == Nil)

      val expected = (1 to n).map(toKey)

      //Convert to sets to ignore ordering
      assert(keys.toSet == expected.toSet)
      ()
    }
  }

  //Create n objects in a pseudoDir, then delete them
  private lazy val doRmTest: String => IO[Unit] = { pseudoDirKey =>
    import cats.implicits._

    val key0 = s"$pseudoDirKey/foo"
    val key1 = s"$pseudoDirKey/bar"

    for {
      key0ExistsInitially <- aws.exists(key0)
      _                   <- aws.put(key0, "foo")
      key0ExistsAfterPut  <- aws.exists(key0)
      _                   <- aws.rm(key0)
      key0ExistsAfterRm   <- aws.exists(key0)
    } yield {
      assert(key0ExistsInitially == false)
      assert(key0ExistsAfterPut)
      assert(key0ExistsAfterRm == false)
      ()
    }
  }

  //Create n objects in a pseudoDir, then delete them
  private def doRmDirTest(n: Int): String => IO[Unit] = { pseudoDirKey =>
    import cats.implicits._

    def toKey(i: Int) = s"${pseudoDirKey}/${i}"

    val pseudoDirKeyWithSlash = s"$pseudoDirKey/"

    for {
      _                  <- (1 to n).toList.map(i => aws.put(toKey(i), i.toString)).sequence
      keysBeforeDeletion <- aws.ls(pseudoDirKeyWithSlash)
      _                  <- aws.rmdir(pseudoDirKeyWithSlash)
      keysAfterDeletion  <- aws.ls(pseudoDirKeyWithSlash)
    } yield {
      val expectedBeforeDeletion = (1 to n).map(toKey)

      //Convert to sets to ignore ordering
      assert(keysBeforeDeletion.toSet == expectedBeforeDeletion.toSet)

      //The "containing folder" was never never explicitly created, so it shouldn't exist
      val expectedAfterDeletion = Nil

      assert(keysAfterDeletion == expectedAfterDeletion)
      ()
    }
  }

  //Put an object, then read it back again
  private def doPutGetTest(pseudoDirKey: String): Unit = {
    import Implicits._

    val pseudoDirKeyWithSlash = s"$pseudoDirKey/"

    val contents = "asdkljaslkdjalskdjklasdj"
    val key      = s"${pseudoDirKey}/some-key"

    val beforePut = bucket.ls(pseudoDirKeyWithSlash)

    //sanity check: the thing we're putting shouldn't have been there yet
    assert(beforePut === Nil)

    bucket.put(key, contents)

    val contentsFromAws = bucket.get(key).getmkString()

    assert(contentsFromAws === contents)
  }
  
  //Put an object, then read it back again
  private lazy val doPutGetFileTest: String => IO[Unit] = { pseudoDirKey =>
    import cats.implicits._
    import Implicits._

    val pseudoDirKeyWithSlash = s"$pseudoDirKey/"

    val file     = Paths.get("src/it/resources/test_upload.txt")
    val key      = s"${pseudoDirKey}/some-key"

    for {
      beforePut       <- aws.ls(pseudoDirKeyWithSlash)
      _               <- aws.put(key, file)
      contentsFromAws <- aws.get(key).map(_.readAsString())
    } yield {
      //sanity check: the thing we're putting shouldn't have been there yet
      assert(beforePut == Nil)

      val fileContents = new String(Files.readAllBytes(file), StandardCharsets.UTF_8)
      
      assert(contentsFromAws == fileContents)
      
      ()
    }
  }


  //Make a pseudo-dir, then make the same one again with different metadata
  private lazy val doMkdirTest: String => IO[Unit] = { pseudoDirKey =>
    import cats.implicits._
    import Implicits._

    val pseudoDirKeyWithSlash = s"$pseudoDirKey/"

    val metadataContents0 = "some-metadata0"
    val metadataContents1 = "some-metadata1"
    
    for {
      _         <- aws.mkdir(pseudoDirKey, metadataContents0)
      metadata0 <- aws.get(s"${pseudoDirKey}/metadata").map(_.readAsString())
      contents0 <- aws.ls(pseudoDirKeyWithSlash)
      _         <- aws.mkdir(pseudoDirKey, metadataContents1)
      metadata1 <- aws.get(s"${pseudoDirKey}/metadata").map(_.readAsString())
      contents1 <- aws.ls(pseudoDirKeyWithSlash)
    } yield {
      assert(metadata0 == metadataContents0)

      //Convert to sets to ignore ordering
      assert(contents0.toSet == Set(s"${pseudoDirKey}/metadata"))

      assert(metadata1 == metadataContents1)

      //Convert to sets to ignore ordering
      assert(contents0.toSet == Set(s"${pseudoDirKey}/metadata"))
      ()
    }
  }
}
