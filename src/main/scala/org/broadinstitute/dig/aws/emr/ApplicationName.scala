package org.broadinstitute.dig.aws.emr

import com.amazonaws.services.elasticmapreduce.model.Application

/** An application that can be pre-installed on the cluster. */
final case class ApplicationName(value: String) {

  /** Create the EMR Application object. */
  def application: Application = new Application().withName(value)
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
