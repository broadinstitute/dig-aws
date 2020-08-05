package org.broadinstitute.dig.aws

import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.emr.Job
import org.scalatest.FunSuite

/**
 * @author clint
 * Jul 27, 2018
 */
trait AwsFunSuite extends FunSuite {
  protected def s3: S3.Bucket
  protected def emr: Emr.Runner

  def testWithPseudoDir(name: String)(body: String => Any): Unit = {
    test(name) {
      val mungedName = name.filter(_ != '/')

      val pseudoDirKey = s"integrationTests/$mungedName"

      def nukeTestDir(): Unit = s3.rm(s"$pseudoDirKey/")

      nukeTestDir()

      body(pseudoDirKey)

      //Test dir will be deleted after successful runs, but will live until the next run
      //if there's a failure.
      nukeTestDir()
    }
  }

  def testWithPseudoDirIO[A](name: String)(body: String => A): Unit = {
    testWithPseudoDir(name)(body)
  }

  def testWithCluster(name: String, scriptResource: String): Unit = {
    test(name) {
      val cluster = ClusterDef(
        name = "IntegrationTest",
        instances = 1,
        masterInstanceType = Ec2.Strategy.generalPurpose(),
      )

      val key = s"resources/$scriptResource"
      val put = s3.putResource(key, scriptResource)
      val uri = s3.s3UriOf(key)
      val step = Job.Script(uri)
      val job = new Job(step)
      val env = Map.empty[String, String]

      emr.runJob(cluster, env, job)
    }
  }
}
