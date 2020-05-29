package org.broadinstitute.dig.aws

import org.scalatest.FunSuite
import java.net.URI
import scala.jdk.CollectionConverters._
import software.amazon.awssdk.services.emr.model.StepConfig
import software.amazon.awssdk.services.emr.model.ActionOnFailure
import software.amazon.awssdk.services.emr.model.HadoopJarStepConfig

final class JobStepTest extends FunSuite {
  import JobStep.toJobName

  test("toJobName - empty basename") {
    import Implicits._

    val uri = new URI("http://example.com")

    assert(uri.basename == "")

    assert(toJobName(uri) == "http://example.com")
  }

  test("toJobName - non-empty basename") {
    val simple = new URI("http://example.com/foo")

    assert(toJobName(simple) == "foo")

    val lessSimple = new URI("http://example.com/foo/bar/baz/blerg")

    assert(toJobName(lessSimple) == "blerg")
  }

  test("Script.config") {
    val uri = new URI("http://example.com/foo/bar/baz/blerg")

    val config = JobStep.Script(uri, "foo", "bar", "baz").config

    assert(config.isInstanceOf[StepConfig])

    assert(config.name == toJobName(uri))
    assert(config.actionOnFailure == ActionOnFailure.TERMINATE_CLUSTER)

    val expectedJarConfig = HadoopJarStepConfig.builder
      .jar("s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar")
      .args(Seq(uri.toString, "foo", "bar", "baz").asJava)
      .build

    assert(config.hadoopJarStep == expectedJarConfig)
  }

  test("PySpark.config") {
    val uri = new URI("http://example.com/foo/bar/baz/blerg")

    val config = JobStep.PySpark(uri, "foo", "bar", "baz").config

    assert(config.isInstanceOf[StepConfig])

    assert(config.name == toJobName(uri))
    assert(config.actionOnFailure == ActionOnFailure.TERMINATE_CLUSTER)

    val expectedJarConfig = HadoopJarStepConfig.builder
      .jar("command-runner.jar")
      .args(Seq("spark-submit", "--deploy-mode", "cluster", uri.toString, "foo", "bar", "baz").asJava)
      .build

    assert(config.hadoopJarStep == expectedJarConfig)
  }

  test("Pig.config") {
    val uri = new URI("http://example.com/foo/bar/baz/blerg")

    val config = JobStep.Pig(uri, "foo" -> "x", "bar" -> "y", "baz" -> "z").config

    assert(config.isInstanceOf[StepConfig])

    assert(config.name == toJobName(uri))
    assert(config.actionOnFailure == ActionOnFailure.TERMINATE_CLUSTER)

    val expectedJarConfig = {
      HadoopJarStepConfig.builder
        .jar("command-runner.jar")
        .args(
          Seq("pig-script",
              "--run-pig-script",
              "--args",
              "-p",
              "foo=x",
              "-p",
              "bar=y",
              "-p",
              "baz=z",
              "-f",
              uri.toString).asJava)
        .build
    }

    assert(config.hadoopJarStep == expectedJarConfig)
  }
}
