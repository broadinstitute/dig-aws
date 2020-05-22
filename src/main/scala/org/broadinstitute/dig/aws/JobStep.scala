package org.broadinstitute.dig.aws

import software.amazon.awssdk.services.emr.model.ActionOnFailure
import software.amazon.awssdk.services.emr.model.HadoopJarStepConfig
import software.amazon.awssdk.services.emr.model.StepConfig
import java.net.URI
import scala.jdk.CollectionConverters._

/** All Hadoop jobs are a series of steps. */
sealed abstract class JobStep {
  protected var env = Seq.empty[(String, String)]

  /** Add a single environment variable binding to this step. */
  def setEnv(key: String, value: String): Unit = env +:= key -> value

  /** Construct the EMR configuration for this step. */
  def config: StepConfig
}

object JobStep {
  import Implicits._

  /** Extract the name of a job from its URI. */
  private def toJobName(uri: URI): String = uri.basename.trim match {
    case ""       => uri.toString
    case nonEmpty => nonEmpty
  }

  /** Create a shell string for setting environment variables. */
  private def environ(env: Seq[(String, String)]): String = {
    env.map { case (key, value) => s"""$key="$value"""" }.mkString(" ")
  }

  /** Use `/bash/sh -c` to construct a command with environment variables set. */
  private def stepCmdLine(args: Seq[String], env: Seq[(String, String)]): List[String] = {
    List("/bin/sh", "-c", s"${environ(env)} ${args.mkString(" ")}")
  }

  /** Create a new Map Reduce step given a JAR (S3 path) the main class to
    * run, and any command line arguments to pass along to the JAR.
    */
  final case class MapReduce(jar: URI, mainClass: String, args: Seq[String]) extends JobStep {
    override def config: StepConfig = {
      val jarConfig = HadoopJarStepConfig.builder
        .jar(jar.toString)
        .mainClass(mainClass)
        .args(stepCmdLine(args, env).asJava)
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
    override def config: StepConfig = {
      val jarConfig = HadoopJarStepConfig.builder
        .jar("command-runner.jar")
        .args(stepCmdLine(args, env).asJava)
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
    override def config: StepConfig = {
      val jarConfig = HadoopJarStepConfig.builder
        .jar("s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar")
        .args(stepCmdLine(script.toString :: args.toList, env).asJava)
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
    override def config: StepConfig = {
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
    override def config: StepConfig = {
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
