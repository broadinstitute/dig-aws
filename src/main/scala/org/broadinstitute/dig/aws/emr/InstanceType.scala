package org.broadinstitute.dig.aws.emr

import org.json4s._
import scala.reflect.ManifestFactory.classType

/** EC2 instance types. Not all instance types are represented as there is a
  * bare minimum required to even run Hadoop/Spark.
  *
  * See: https://aws.amazon.com/ec2/instance-types/
  */
final case class InstanceType(value: String)

/** Companion object with defined instances. */
object InstanceType {

  /** General-purpose, balanced. */
  val m5_2xlarge  = InstanceType("m5.2xlarge")
  val m5_4xlarge  = InstanceType("m5.4xlarge")
  val m5_12xlarge = InstanceType("m5.12xlarge")
  val m5_24xlarge = InstanceType("m5.24xlarge")

  /** Compute-optimized. */
  val c5_2xlarge  = InstanceType("c5.2xlarge")
  val c5_4xlarge  = InstanceType("c5.4xlarge")
  val c5_9xlarge  = InstanceType("c5.9xlarge")
  val c5_18xlarge = InstanceType("c5.18xlarge")

  /** Convert a JSON value to an InstanceType. */
  val deserialize: PartialFunction[JValue, InstanceType] = {
    case JString("m5.2xlarge")  => m5_2xlarge
    case JString("m5.4xlarge")  => m5_4xlarge
    case JString("m5.12xlarge") => m5_12xlarge
    case JString("m5.24xlarge") => m5_24xlarge
    case JString("c5.2xlarge")  => c5_2xlarge
    case JString("c5.4xlarge")  => c5_4xlarge
    case JString("c5.9xlarge")  => c5_9xlarge
    case JString("c5.18xlarge") => c5_18xlarge
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
