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
  def fromTypesafeConfig(config: Config, configPath: String): Try[AWSConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders._

    //NB: Ficus unmarshals the contents of the Config object at the key given by `configPath` into an AWSConfig 
    //instance.  Names of fields in AWSConfig and keys under the key given by `configPath` must match.
    Try(config.as[AWSConfig](configPath))
  }
}
