package org.broadinstitute.dig.aws.config.emr

import org.json4s._

/** The version ID of the EMR cluster to create.
  */
final case class ReleaseLabel(value: String) {
  require(value.startsWith("emr-"), s"Invalid EMR release ID: '$value'")
}

/** Companion object with a default EMR release ID that can be used.
  */
object ReleaseLabel {

  /** Default EMR instance release version. */
  val emrLatest: ReleaseLabel = ReleaseLabel("emr-5.30.0")

  /** Convert a JSON value to an ReleaseLabel. */
  val deserialize: PartialFunction[JValue, ReleaseLabel] = {
    case JString(value) => ReleaseLabel(value)
  }

  /** Convert an ReleaseLabel to a JSON value. */
  val serialize: PartialFunction[Any, JValue] = {
    case ReleaseLabel(value) => JString(value)
  }

  /** Custom serializer for ReleaseLabel. To use this, add it to the default
    * formats when de-serializing...
    *
    * implicit val formats = json4s.DefaultFormats + ReleaseLabel.Serializer
    */
  case object Serializer extends CustomSerializer[ReleaseLabel](format => deserialize -> serialize)
}
