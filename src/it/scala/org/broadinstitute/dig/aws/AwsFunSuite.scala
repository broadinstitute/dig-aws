package org.broadinstitute.dig.aws

import org.broadinstitute.dig.aws.emr._
import org.scalatest.FunSuite
import java.time.format.DateTimeFormatter
import java.time.ZoneId
import java.time.Instant
import cats.effect.IO

/**
 * @author clint
 * Jul 27, 2018
 */
trait AwsFunSuite extends FunSuite {
  protected def aws: Aws

  def testWithPseudoDir(name: String)(body: String => Any): Unit = {
    test(name) {
      val mungedName = name.filter(_ != '/')

      val pseudoDirKey = s"integrationTests/${mungedName}"

      def nukeTestDir() = aws.rmdir(s"${pseudoDirKey}/").unsafeRunSync()

      nukeTestDir()

      body(pseudoDirKey)

      //Test dir will be deleted after successful runs, but will live until the next run
      //if there's a failure.
      nukeTestDir()
    }
  }

  def testWithPseudoDirIO[A](name: String)(body: String => IO[A]): Unit = {
    testWithPseudoDir(name)(body(_).unsafeRunSync())
  }

  def testWithCluster(name: String, scriptResource: String): Unit = {
    test(name) {
      val cluster = Cluster(
        name = "IntegrationTest",
        instances = 1,
        masterInstanceType = InstanceType.m5_2xlarge,
      )

      val ioa = for {
        uri <- aws.upload(scriptResource)
        job <- aws.runJob(cluster, JobStep.Script(uri))
        res <- aws.waitForJob(job)
      } yield ()

      // this will assert in waitForJob if there's an error
      ioa.unsafeRunSync()
    }
  }
}
