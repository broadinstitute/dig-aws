package org.broadinstitute.dig.aws.config.emr

import org.json4s._


/** Security groups define access to the EMR cluster machines.
  */
final case class SubnetId(value: String) {
  require(value.startsWith("subnet-"), s"Invalid subnet: '$value'")
}

/** Companion object with serialization formats.
  */
object SubnetId {

  /** Convert a JSON value to an SubnetId. */
  val deserialize: PartialFunction[JValue, SubnetId] = {
    case JString(value) => SubnetId(value)
  }

  /** Convert an SubnetId to a JSON value. */
  val serialize: PartialFunction[Any, JValue] = {
    case SubnetId(value) => JString(value)
  }

  /** Custom serializer for SubnetId. To use this, add it to the default
    * formats when de-serializing...
    *
    * implicit val formats = json4s.DefaultFormats + SubnetId.Serializer
    */
  case object Serializer extends CustomSerializer[SubnetId](format => deserialize -> serialize)
}
