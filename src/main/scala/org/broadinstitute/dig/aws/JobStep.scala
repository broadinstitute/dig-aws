package org.broadinstitute.dig.aws

import software.amazon.awssdk.services.emr.model.ActionOnFailure
import software.amazon.awssdk.services.emr.model.HadoopJarStepConfig
import software.amazon.awssdk.services.emr.model.StepConfig
import java.net.URI
import scala.collection.JavaConverters._

/** All Hadoop jobs are a series of steps. */
sealed trait JobStep {
  val config: StepConfig
}

object JobStep {
  import Implicits._

  def toJobName(uri: URI): String = uri.basename.trim match {
    case ""       => uri.toString
    case nonEmpty => nonEmpty
  }

  /** Create a new Map Reduce step given a JAR (S3 path) the main class to
    * run, and any command line arguments to pass along to the JAR.
    */
  final case class MapReduce(jar: URI, mainClass: String, args: Seq[String]) extends JobStep {
    val config: StepConfig = {
      val jarConfig = HadoopJarStepConfig.builder
        .jar(jar.toString)
        .mainClass(mainClass)
        .args(args.asJava)
        .build

      StepConfig.builder
        .name(mainClass)
        .actionOnFailure(ActionOnFailure.TERMINATE_CLUSTER)
        .hadoopJarStep(jarConfig)
        .build
    }
  }

  /** Create a new JAR step that uses the built-in Command Runner supplied by
    * AWS. This is used to spawn Spark, Pig, and more... see:
    * https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-commandrunner.html
    */
  final case class CommandRunner(name: String, args: Seq[String]) extends JobStep {
    val config: StepConfig = {
      val jarConfig = HadoopJarStepConfig.builder
        .jar("command-runner.jar")
        .args(args.asJava)
        .build

      StepConfig.builder
        .name(name)
        .actionOnFailure(ActionOnFailure.TERMINATE_CLUSTER)
        .hadoopJarStep(jarConfig)
        .build
    }
  }

  /** Create Script Runner step that will execute a generic script... see:
    * https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hadoop-script.html
    */
  final case class Script(script: URI, args: String*) extends JobStep {
    val config: StepConfig = {
      val jarConfig = HadoopJarStepConfig.builder
        .jar("s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar")
        .args((script.toString :: args.toList).asJava)
        .build

      StepConfig.builder
        .name(toJobName(script))
        .actionOnFailure(ActionOnFailure.TERMINATE_CLUSTER)
        .hadoopJarStep(jarConfig)
        .build
    }
  }

  /** Create a Command Runner step that will run a Python3 spark script.
    */
  final case class PySpark(script: URI, args: String*) extends JobStep {
    val config: StepConfig = {
      val commandRunnerArgs = List(
        "spark-submit",
        "--deploy-mode",
        "cluster",
        script.toString
      )

      // use the basename of the file as the name of the step
      CommandRunner(toJobName(script), commandRunnerArgs ++ args).config
    }
  }

  /** Create a Command Runner step that will run a Pig script.
    *
    * Pig script parameters are passed in with `-p key=value` for each and
    * every argument, and the script is finally passed with `-f file`.
    */
  final case class Pig(script: URI, args: (String, String)*) extends JobStep {
    val config: StepConfig = {
      val commandRunnerArgs = List(
        "pig-script",
        "--run-pig-script",
        "--args"
      )

      // intersperse -p with key=value parameters
      val params = args.map { case (k, v) => s"$k=$v" }.flatMap(List("-p", _))

      // prepend the script location with the file flag
      val file = List("-f", script.toString)

      // use the basename of the script location as the name of the step
      CommandRunner(toJobName(script), commandRunnerArgs ++ params ++ file).config
    }
  }
}
