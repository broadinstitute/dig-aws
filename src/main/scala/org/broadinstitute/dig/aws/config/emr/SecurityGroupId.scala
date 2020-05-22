package org.broadinstitute.dig.aws.config.emr

import org.json4s._

/** Security groups define access to the EMR cluster machines.
  */
final case class SecurityGroupId(value: String) {
  require(value.startsWith("sg-"), s"Invalid security group: '$value'")
}

/** Companion object with serialization formats.
  */
object SecurityGroupId {

  /** Convert a JSON value to an SecurityGroupId. */
  val deserialize: PartialFunction[JValue, SecurityGroupId] = {
    case JString(value) => SecurityGroupId(value)
  }

  /** Convert an SecurityGroupId to a JSON value. */
  val serialize: PartialFunction[Any, JValue] = {
    case SecurityGroupId(value) => JString(value)
  }

  /** Custom serializer for SecurityGroupId. To use this, add it to the default
    * formats when de-serializing...
    *
    * implicit val formats = json4s.DefaultFormats + SecurityGroupId.Serializer
    */
  case object Serializer extends CustomSerializer[SecurityGroupId](format => deserialize -> serialize)
}
