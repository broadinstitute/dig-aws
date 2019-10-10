package org.broadinstitute.dig.aws

import org.broadinstitute.dig.aws.emr._
import org.scalatest.FunSuite
import java.time.format.DateTimeFormatter
import java.time.ZoneId
import java.time.Instant

import scala.language.higherKinds

/**
 * @author clint
 * Jul 27, 2018
 */
abstract class AwsFunSuite[F[_]](implicit protected val awsOps: AwsOps[F]) extends FunSuite {
  protected def aws: AWS[F]

  import awsOps.Implicits._
  
  def testWithPseudoDir(name: String)(body: String => Any): Unit = {
    test(name) {
      val mungedName = name.filter(_ != '/')

      val pseudoDirKey = s"integrationTests/${mungedName}"

      def nukeTestDir() = aws.rmdir(s"${pseudoDirKey}/").run()

      nukeTestDir()

      body(pseudoDirKey)

      //Test dir will be deleted after successful runs, but will live until the next run
      //if there's a failure.
      nukeTestDir()
    }
  }

  def testWithPseudoDirF[A](name: String)(body: String => F[A]): Unit = {
    testWithPseudoDir(name)(body(_).run())
  }

  def testWithCluster(name: String, scriptResource: String): Unit = {
    test(name) {
      val cluster = Cluster(
        name = "IntegrationTest",
        instances = 1,
        masterInstanceType = InstanceType.m5_2xlarge,
      )

      val fa = for {
        uri <- aws.upload(scriptResource)
        job <- aws.runJob(cluster, JobStep.Script(uri))
        res <- aws.waitForJob(job)
      } yield ()

      // this will assert in waitForJob if there's an error
      fa.run()
    }
  }
}
