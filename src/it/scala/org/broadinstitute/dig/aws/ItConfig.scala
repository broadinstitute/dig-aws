package org.broadinstitute.dig.aws

import org.broadinstitute.dig.aws.config.AwsConfig
import org.json4s.JsonAST.JNothing
import org.json4s.jackson.JsonMethods.parse

import scala.io.Source

/** Loads `src/it/resources/config.json` (git-ignored) for the integration tests.
  *
  * The file is the aggregator's config.json, which nests the AWS settings under
  * an `aws` key (that is how dig-aggregator-core reads it). A bare `AwsConfig`
  * JSON, with `project`, `emr`, ... at the top level, is accepted too.
  */
object ItConfig {
  def load(resource: String = "config.json"): AwsConfig = {
    implicit val formats = AwsConfig.formats

    val source = Source.fromResource(resource)
    val json   = try parse(source.mkString) finally source.close()

    (json \ "aws" match {
      case JNothing => json
      case nested   => nested
    }).extract[AwsConfig]
  }
}
