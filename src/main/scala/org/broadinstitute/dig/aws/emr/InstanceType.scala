package org.broadinstitute.dig.aws.emr

import org.json4s._
import scala.reflect.ManifestFactory.classType

/** EC2 instance types. Not all instance types are represented as there is a
  * bare minimum required to even run Hadoop/Spark.
  *
  * See: https://aws.amazon.com/ec2/instance-types/
  */
final case class InstanceType(Label: String) {
  def value: String = Label
}

/** Companion object with defined instances. */
object InstanceType {

  /** General-purpose, balanced. */
  val m5_2xlarge  = InstanceType("m5.2xlarge")
  val m5_4xlarge  = InstanceType("m5.4xlarge")
  val m5_12xlarge = InstanceType("m5.12xlarge")
  val m5_24xlarge = InstanceType("m5.24xlarge")

  /** Memory-optimized. */
  val r5_2xlarge  = InstanceType("r5.2xlarge")
  val r5_4xlarge  = InstanceType("r5.4xlarge")
  val r5_8xlarge = InstanceType("r5.8xlarge")
  val r5_12xlarge = InstanceType("r5.12xlarge")

  /** Compute-optimized. */
  val c5_2xlarge  = InstanceType("c5.2xlarge")
  val c5_4xlarge  = InstanceType("c5.4xlarge")
  val c5_9xlarge  = InstanceType("c5.9xlarge")
  val c5_18xlarge = InstanceType("c5.18xlarge")

  /** Convert a JSON value to an InstanceType. */
  val deserialize: PartialFunction[JValue, InstanceType] = {
    case JString(m5_2xlarge.Label)  => m5_2xlarge
    case JString(m5_4xlarge.Label)  => m5_4xlarge
    case JString(m5_12xlarge.Label) => m5_12xlarge
    case JString(m5_24xlarge.Label) => m5_24xlarge
    case JString(r5_2xlarge.Label)  => r5_2xlarge
    case JString(r5_4xlarge.Label)  => r5_4xlarge
    case JString(r5_8xlarge.Label) => r5_8xlarge
    case JString(r5_12xlarge.Label) => r5_12xlarge
    case JString(c5_2xlarge.Label)  => c5_2xlarge
    case JString(c5_4xlarge.Label)  => c5_4xlarge
    case JString(c5_9xlarge.Label)  => c5_9xlarge
    case JString(c5_18xlarge.Label) => c5_18xlarge
  }

  /** Convert an InstanceType to a JSON value. */
  val serialize: PartialFunction[Any, JValue] = {
    case InstanceType(value) => JString(value)
  }

  /** Custom serializer for InstanceType. To use this, add it to the default
    * formats when deserializing...
    *
    * implicit val formats = json4s.DefaultFormats + InstanceType.Serializer
    */
  case object Serializer extends CustomSerializer[InstanceType](format => deserialize -> serialize)
}
