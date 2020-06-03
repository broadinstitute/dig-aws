package org.broadinstitute.dig.aws

import org.scalatest.FunSuite
import java.net.URI
import scala.jdk.CollectionConverters._
import software.amazon.awssdk.services.emr.model.StepConfig
import software.amazon.awssdk.services.emr.model.ActionOnFailure
import software.amazon.awssdk.services.emr.model.HadoopJarStepConfig

final class JobStepTest extends FunSuite {
  import JobStep.toJobName

  test("step equality") {
    val mrStep1 = JobStep.MapReduce(new URI("http://foo/bar/mr"), "main", Seq("p1", "p2"))
    val pigStep1 = JobStep.Pig(new URI("http://foo/bar/pig"), "p1" -> "a", "p2" -> "b")
    val sparkStep1 = JobStep.PySpark(new URI("http://foo/bar/spark"), "arg1", "arg2")
    val scriptStep1 = JobStep.PySpark(new URI("http://foo/bar/script"), "arg1", "arg2")
    val mrStep2 = JobStep.MapReduce(new URI("http://foo/bar/mr"), "main", Seq("p1", "p2"))
    val pigStep2 = JobStep.Pig(new URI("http://foo/bar/pig"), "p1" -> "a", "p2" -> "b")
    val sparkStep2 = JobStep.PySpark(new URI("http://foo/bar/spark"), "arg1", "arg2")
    val scriptStep2 = JobStep.PySpark(new URI("http://foo/bar/script"), "arg1", "arg2")
    val mrStep3 = JobStep.MapReduce(new URI("http://foo/bar/mr"), "main2", Seq("p1", "p2", "p3"))

    assert(mrStep1 == mrStep2)
    assert(pigStep1 == pigStep2)
    assert(sparkStep1 == sparkStep2)
    assert(scriptStep1 == scriptStep2)
    assert(mrStep1 != mrStep3)
    assert(pigStep1 != sparkStep2)
    assert(sparkStep1 != scriptStep2)
    assert(scriptStep1 != pigStep2)
  }

  test("job equality") {
    val job1 = Seq(
      JobStep.MapReduce(new URI("http://foo/bar/mr"), "main", Seq("p1", "p2")),
      JobStep.Pig(new URI("http://foo/bar/pig"), "p1" -> "a", "p2" -> "b"),
      JobStep.PySpark(new URI("http://foo/bar/spark"), "arg1", "arg2"),
      JobStep.PySpark(new URI("http://foo/bar/script"), "arg1", "arg2"),
    )

    val job2 = Seq(
      JobStep.MapReduce(new URI("http://foo/bar/mr"), "main", Seq("p1", "p2")),
      JobStep.Pig(new URI("http://foo/bar/pig"), "p1" -> "a", "p2" -> "b"),
      JobStep.PySpark(new URI("http://foo/bar/spark"), "arg1", "arg2"),
      JobStep.PySpark(new URI("http://foo/bar/script"), "arg1", "arg2"),
    )

    assert(job1 == job2)
    assert(job1.drop(1) != job2)
    assert(job1.take(1) != job2)
  }

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
