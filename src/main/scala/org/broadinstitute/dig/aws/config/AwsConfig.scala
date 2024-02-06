package org.broadinstitute.dig.aws.config

import java.io.File

import org.json4s.{CustomSerializer, DefaultFormats, Formats}
import org.json4s.jackson.Serialization.read

import scala.io.Source
import scala.util.Try

/** AWS configuration settings. */
final case class AwsConfig(
  input: S3Config,
  output: S3Config,
  bioindex: S3Config,
  emr: EmrConfig,
  runs: RdsConfig,
  portal: RdsConfig
)

/** Companion object used for loading configuration files. */
object AwsConfig {
  implicit val formats: Formats = DefaultFormats ++ EmrConfig.customSerializers

  /** Custom JSON serializers needed for parsing JSON. */
  val customSerializers: Seq[CustomSerializer[_]] = EmrConfig.customSerializers

  /** Load a JSON file and parse it. */
  def load(file: File): Try[AwsConfig] = Try {
    val source   = Source.fromFile(file)
    val settings = read[AwsConfig](source.mkString)

    source.close
    settings
  }

  /** Load a JSON from a resource and parse it. */
  def loadFromResource(resource: String): Try[AwsConfig] = Try {
    val source = Source.fromResource(resource)
    val settings = read[AwsConfig](source.mkString)

    settings
  }
}
