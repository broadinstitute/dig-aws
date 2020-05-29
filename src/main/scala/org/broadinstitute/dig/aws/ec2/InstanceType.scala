package org.broadinstitute.dig.aws.ec2

import org.json4s._

/** The instance type of an EC2 instance.
  *
  * See: https://aws.amazon.com/ec2/instance-types/
  */
case class InstanceType(value: String)

/** Companion object with global AMIs.
  */
object InstanceType {

  /** The default instance type. */
  val default: InstanceType = GeneralPurpose.m2xlarge

  /** Compute strategies. */
  sealed case class Strategy(prefix: String)

  /** General-purpose instances. */
  object GeneralPurpose extends Strategy("m5") {
    val m2xlarge: InstanceType = InstanceType("m5n.2xlarge")
    val m4xlarge: InstanceType = InstanceType("m5n.4xlarge")
    val m8xlarge: InstanceType = InstanceType("m5n.8xlarge")
    val m12xlarge: InstanceType = InstanceType("m5n.12xlarge")
    val m16xlarge: InstanceType = InstanceType("m5n.16xlarge")
  }

  /** Memory-optimized instances. */
  object MemoryOptimized extends Strategy("r5") {
    val r2xlarge: InstanceType = InstanceType("r5n.2xlarge")
    val r4xlarge: InstanceType = InstanceType("r5n.4xlarge")
    val r8xlarge: InstanceType = InstanceType("r5n.8xlarge")
    val r12xlarge: InstanceType = InstanceType("r5n.12xlarge")
    val r16xlarge: InstanceType = InstanceType("r5n.16xlarge")
  }

  /** CPU-optimized instances. */
  object ComputeOptimized extends Strategy("c5") {
    val c2xlarge: InstanceType = InstanceType("c5n.2xlarge")
    val c4xlarge: InstanceType = InstanceType("c5n.4xlarge")
    val c8xlarge: InstanceType = InstanceType("c5n.9xlarge")
    val c18xlarge: InstanceType = InstanceType("c5n.18xlarge")
  }

  /** Instances with GPU accelerated capabilities. */
  object GpuAccelerated extends Strategy("g4") {
    val g4xlarge: InstanceType = InstanceType("g4dn.4xlarge")
    val g8xlarge: InstanceType = InstanceType("g4dn.8xlarge")
    val g16xlarge: InstanceType = InstanceType("g4dn.16xlarge")
  }

  /** Convert a JSON value to an InstanceType. */
  val deserialize: PartialFunction[JValue, InstanceType] = {
    case JString(value) => InstanceType(value)
  }

  /** Convert an InstanceType to a JSON value. */
  val serialize: PartialFunction[Any, JValue] = {
    case InstanceType(value) => JString(value)
  }

  /** Custom serializer for AmiId. To use this, add it to the default
    * formats when de-serializing...
    *
    * implicit val formats = json4s.DefaultFormats + AmiId.Serializer
    */
  case object Serializer extends CustomSerializer[InstanceType](format => deserialize -> serialize)
}
