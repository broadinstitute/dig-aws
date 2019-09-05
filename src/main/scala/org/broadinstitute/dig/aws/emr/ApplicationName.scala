package org.broadinstitute.dig.aws.emr

import software.amazon.awssdk.services.emr.model.Application

/** An application that can be pre-installed on the cluster. */
final case class ApplicationName(value: String) {

  /** Create the EMR Application object. */
  def application: Application = Application.builder.name(value).build
}

/** Companion object for cluster applications. */
object ApplicationName {

  /** Applications understood by AWS to be installed with the cluster. */
  val hadoop: ApplicationName = ApplicationName("Hadoop")
  val spark: ApplicationName  = ApplicationName("Spark")
  val hive: ApplicationName   = ApplicationName("Hive")
  val pig: ApplicationName    = ApplicationName("Pig")
  val hue: ApplicationName    = ApplicationName("Hue")
}
