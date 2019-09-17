package org.broadinstitute.dig.aws.config

import scala.util.Try

import org.broadinstitute.dig.aws.config.emr.EmrConfig

import com.typesafe.config.Config

/** AWS configuration settings. */
final case class AWSConfig(
    s3: S3Config,
    emr: EmrConfig,
)

object AWSConfig {
  def fromConfig(config: Config, configPath: String): Try[AWSConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders._

    //NB: Ficus marshals the contents of loamstream.aws into a GoogleCloudConfig instance.
    //Names of fields in GoogleCloudConfig and keys under loamstream.googlecloud must match.
    Try(config.as[AWSConfig](configPath))
  }
}
