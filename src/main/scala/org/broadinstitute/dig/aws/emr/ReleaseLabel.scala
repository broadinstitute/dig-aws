package org.broadinstitute.dig.aws.emr

/** The version ID of the EMR cluster to create.
  */
final case class ReleaseLabel(value: String) {
  require(value.startsWith("emr-"), s"Invalid EMR release ID: '$value'")
}

/** Companion object with a default EMR release ID that can be used.
  */
object ReleaseLabel {

  /** Default EMR instance release version. */
  val emrDefault: ReleaseLabel = ReleaseLabel("emr-5.30.0")

  /** The latest EMR instance release version. */
  val emrLatest: ReleaseLabel = ReleaseLabel("emr-6.0.0")
}
