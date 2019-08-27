package org.broadinstitute.dig.aws.config

import org.broadinstitute.dig.aws.config.emr.EmrConfig

/** AWS configuration settings. */
final case class AWSConfig(
    s3: S3Config,
    emr: EmrConfig,
)

/** S3 configuration settings. */
final case class S3Config(
    bucket: String,
)
