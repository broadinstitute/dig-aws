package org.broadinstitute.dig.aws

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant

import scala.io.Source

import org.broadinstitute.dig.aws.config.AWSConfig
import org.broadinstitute.dig.aws.config.emr.EmrConfig
import org.json4s.DefaultFormats
import org.json4s.Formats
import org.json4s.jackson.Serialization.read

import cats.effect.IO
import cats.implicits._


/**
  * @author clint
  * Jul 27, 2018
  */
final class AwsTest extends AwsFunSuite {
  
  override protected val aws: AWS = {
    //Config file needs to be in place before this test will work.
    val configFile = new File("src/it/resources/config.json")
    
    val configFileContents = Source.fromFile(configFile).mkString
    
    implicit val formats: Formats = DefaultFormats ++ EmrConfig.customSerializers

    /** Load the settings file and parse it. */
    val awsConfig = read[AWSConfig](configFileContents)
    
    new AWS(awsConfig)
  }
  
  testWithPseudoDirIO("ETagLastModified") {
    doETagLastModifiedTest
  }

  /**
    * Create a cluster and run a simple script job.
    */
  testWithCluster("Simple Cluster", "test_script.py")

  /**
    * Put() an object, then get() it
    */
  testWithPseudoDirIO("PutGet") {
    doPutGetTest
  }
  
  /**
		* Put() a file, then get() it
		*/
  testWithPseudoDirIO("PutGetFile") {
    doPutGetFileTest
  }

  /**
    * Make a pseudo-dir, then make the same one again with different metadata
    */
  testWithPseudoDirIO("Mkdir") {
    doMkdirTest
  }

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
    */
  testWithPseudoDirIO("PutLs2500") {
    doPutLsTest(2500)
  }

  /**
    * Create an object and delete it
    */
  testWithPseudoDirIO("RmExists") {
    doRmTest
  }

  /**
    * Create 10 objects in a pseudo-dir and delete them
    */
  testWithPseudoDirIO("RmDir10") {
    doRmDirTest(10)
  }

  /**
    * Create 2500 objects in a pseudo-dir and delete them
    */
  testWithPseudoDirIO("RmDir2500") {
    doRmDirTest(2500)
  }

  testWithPseudoDirIO("Upload") {
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
  private lazy val doPutGetTest: String => IO[Unit] = { pseudoDirKey =>
    import cats.implicits._
    import Implicits._

    val pseudoDirKeyWithSlash = s"$pseudoDirKey/"

    val contents = "asdkljaslkdjalskdjklasdj"
    val key      = s"${pseudoDirKey}/some-key"

    for {
      beforePut       <- aws.ls(pseudoDirKeyWithSlash)
      _               <- aws.put(key, contents)
      contentsFromAws <- aws.get(key).map(_.readAsString())
    } yield {
      //sanity check: the thing we're putting shouldn't have been there yet
      assert(beforePut == Nil)

      assert(contentsFromAws == contents)
      ()
    }
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
  
  private lazy val doETagLastModifiedTest: String => IO[Unit] = { pseudoDirKey =>
    val key = s"${pseudoDirKey}/foo.txt"
    
    val now = Instant.now
    
    for {
      missingETag <- aws.eTagOf(key)
      missingLastModified <- aws.lastModifiedTimeOf(key)
      nonExistentKeyExists <- aws.exists(key)
      _ <- aws.put(key, Paths.get("src/it/resources/test_upload.txt"))
      eTag <- aws.eTagOf(key)
      lastModified <- aws.lastModifiedTimeOf(key)
    } yield {
      assert(nonExistentKeyExists == false)
      assert(missingLastModified === None)
      assert(missingETag == None)
      
      assert(eTag.get == "76323bb2bba387d27f9e2a5d17c03ac2")
      assert(lastModified.get.toEpochMilli >= now.toEpochMilli)
      
      ()
    }
  }
}
