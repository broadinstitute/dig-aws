package org.broadinstitute.dig.aws

import java.net.URI

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.config.EmrConfig
import org.broadinstitute.dig.aws.config.emr.{SecurityGroupId, SubnetId}
import org.broadinstitute.dig.aws.emr._
import org.scalatest.FunSuite
import software.amazon.awssdk.services.ec2.model.InstanceType

import scala.io.Source

/**
 * @author clint
 * Jul 27, 2018
 */
trait AwsFunSuite extends FunSuite with LazyLogging {
  protected val bucketName: String = "dig-integration-tests"

  protected val bucket: S3.Bucket = new S3.Bucket(bucketName)

  protected def emrRunnerFor(cluster: ClusterDef): Emr.Runner = {
    val emrConfig = EmrConfig(
      sshKeyName = "GenomeStore REST",
      subnetId = SubnetId("subnet-ab89bbf3"),
      securityGroupIds = Seq(SecurityGroupId("sg-2b58c961")))

    val logBucketUri: URI = bucket.s3UriOf(s"logs/${cluster.name}")

    new Emr.Runner(emrConfig, logBucketUri.toString)
  }

  def testWithPseudoDir(name: String)(body: String => Any): Unit = {
    test(name) {
      val mungedName = name.filter(_ != '/')

      val pseudoDirKey = s"integrationTests/${mungedName}"

      def nukeTestDir(): Unit = bucket.rm(s"${pseudoDirKey}/")

      nukeTestDir()

      body(pseudoDirKey)

      //Test dir will be deleted after successful runs, but will live until the next run
      //if there's a failure.
      nukeTestDir()
    }
  }

  def testWithCluster(name: String, scriptResource: String): Unit = {
    test(name) {
      val cluster = ClusterDef(
        name = "IntegrationTest",
        instances = 1,
        masterInstanceType = Strategy(InstanceType.M5_2_XLARGE))

      val uri = uploadResource(scriptResource)

      emrRunnerFor(cluster).runJob(cluster, env = Map.empty, JobStep.Script(uri))
    }
  }

  private def uploadResource(resource: String, dirKey: String = "resources"): URI = {
    val key = s"""${dirKey}/${resource.stripPrefix("/")}"""

    import org.broadinstitute.dig.aws.util.CanBeClosed.using

    //Produce the contents of the classpath resource at `resource` as a string,
    //and will close the InputStream backed by the resource when reading the resource's data is
    //done, either successfully or due to an error.
    val contents: String = {
      using(getClass.getClassLoader.getResourceAsStream(resource)) { stream =>
        using(Source.fromInputStream(stream)) {
          _.mkString.replace("\r\n", "\n")
        }
      }
    }

    logger.debug(s"Uploading $resource to S3...")

    bucket.put(key, contents)

    bucket.s3UriOf(key)
  }
}
