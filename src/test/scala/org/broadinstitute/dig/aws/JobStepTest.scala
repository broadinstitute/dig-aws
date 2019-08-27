package org.broadinstitute.dig.aws

import org.scalatest.FunSuite
import java.net.URI
import com.amazonaws.services.elasticmapreduce.model.StepConfig
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig
import org.scalactic.source.Position.apply
import scala.collection.Seq

/**
 * @author clint
 * Aug 27, 2018
 */
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
    import scala.collection.JavaConverters._

    val uri = new URI("http://example.com/foo/bar/baz/blerg")

    val config = JobStep.Script(uri, "foo", "bar", "baz").config

    assert(config.isInstanceOf[StepConfig])

    assert(config.getName == toJobName(uri))
    assert(config.getActionOnFailure == ActionOnFailure.TERMINATE_CLUSTER.name)

    val expectedJarConfig = (new HadoopJarStepConfig)
      .withJar("s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar")
      .withArgs(Seq(uri.toString, "foo", "bar", "baz").asJava)

    assert(config.getHadoopJarStep == expectedJarConfig)
  }

  test("PySpark.config") {
    import scala.collection.JavaConverters._

    val uri = new URI("http://example.com/foo/bar/baz/blerg")

    val config = JobStep.PySpark(uri, "foo", "bar", "baz").config

    assert(config.isInstanceOf[StepConfig])

    assert(config.getName == toJobName(uri))
    assert(config.getActionOnFailure == ActionOnFailure.TERMINATE_CLUSTER.name)

    val expectedJarConfig = (new HadoopJarStepConfig)
      .withJar("command-runner.jar")
      .withArgs(Seq("spark-submit", "--deploy-mode", "cluster", uri.toString, "foo", "bar", "baz").asJava)

    assert(config.getHadoopJarStep == expectedJarConfig)
  }

  test("Pig.config") {
    import scala.collection.JavaConverters._

    val uri = new URI("http://example.com/foo/bar/baz/blerg")

    val config = JobStep.Pig(uri, "foo" -> "x", "bar" -> "y", "baz" -> "z").config

    assert(config.isInstanceOf[StepConfig])

    assert(config.getName == toJobName(uri))
    assert(config.getActionOnFailure == ActionOnFailure.TERMINATE_CLUSTER.name)

    val expectedJarConfig =
      (new HadoopJarStepConfig)
        .withJar("command-runner.jar")
        .withArgs(
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

    assert(config.getHadoopJarStep == expectedJarConfig)
  }
}
