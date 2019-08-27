package org.broadinstitute.dig.aws.config.emr

import org.json4s._

/** Custom role ID for a cluster setting.
  */
final case class RoleId(value: String)

/** Companion object containing global roles.
  */
object RoleId {

  /** The default roles that are constant. */
  val defaultRole: RoleId            = RoleId("EMR_DefaultRole")
  val ec2DefaultRole: RoleId         = RoleId("EMR_EC2_DefaultRole")
  val autoScalingDefaultRole: RoleId = RoleId("EMR_AutoScaling_DefaultRole")

  /** Convert a JSON value to an RoleId. */
  val deserialize: PartialFunction[JValue, RoleId] = {
    case JString(value) => RoleId(value)
  }

  /** Convert an RoleId to a JSON value. */
  val serialize: PartialFunction[Any, JValue] = {
    case RoleId(value) => JString(value)
  }

  /** Custom serializer for RoleId. To use this, add it to the default
    * formats when deserializing...
    *
    * implicit val formats = json4s.DefaultFormats + RoleId.Serializer
    */
  case object Serializer extends CustomSerializer[RoleId](format => deserialize -> serialize)
}
