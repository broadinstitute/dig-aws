package org.broadinstitute.dig.aws.emr

import org.json4s._

/** When creating an EMR cluster, you provide the AMI (EC2 image instance)
  * to clone for the master and slave nodes.
  */
final case class AmiId(value: String) {
  require(value.startsWith("ami-"), s"Invalid AMI ID: '$value'")
}

/** Companion object with global AMIs.
  */
object AmiId {

  /** Constant AMI provided by AWS with nothing special. */
  val amazonLinux_2018_3: AmiId = AmiId("ami-f316478c")

  /** Convert a JSON value to an AmiId. */
  val deserialize: PartialFunction[JValue, AmiId] = {
    case JString(value) => AmiId(value)
  }

  /** Convert an AmiId to a JSON value. */
  val serialize: PartialFunction[Any, JValue] = {
    case AmiId(value) => JString(value)
  }

  /** Custom serializer for AmiId. To use this, add it to the default
    * formats when deserializing...
    *
    * implicit val formats = json4s.DefaultFormats + AmiId.Serializer
    */
  case object Serializer extends CustomSerializer[AmiId](format => deserialize -> serialize)
}
