package org.broadinstitute.dig.aws.config.emr

import org.json4s.JsonAST._
import org.json4s.CustomSerializer
import scala.util.control.NonFatal

/**
  * @author clint
  * @date Sep 13, 2021
  * 
  * @param value
  */
sealed abstract class EbsVolumeType(val value: String)

object EbsVolumeType {
  case object Gp2 extends EbsVolumeType("gp2")
  case object Io1 extends EbsVolumeType("io1")
  case object Standard extends EbsVolumeType("standard")

  lazy val values: Iterable[EbsVolumeType] = valuesByName.values

  private lazy val valuesByName: Map[String, EbsVolumeType] = {
    Seq(Gp2, Io1, Standard).map(evt => evt.value -> evt).toMap
  }

  private def unsafeFromString(s: String): EbsVolumeType = {
    try { 
      valuesByName(s.trim.toLowerCase)
    } catch {
      case NonFatal(e) => {
        throw new Exception(s"Unknown EbsVolumeType '$s'; possible values are ${valuesByName.keys.mkString(",")}")
      }
    }
  }

  object JsonSupport {
    /** Convert a JSON value to an EbsVolumeType. */
    val deserialize: PartialFunction[JValue, EbsVolumeType] = {
      case JString(value) => unsafeFromString(value)
    }

    /** Convert an EbsVolumeType to a JSON value. */
    val serialize: PartialFunction[Any, JValue] = {
      case e: EbsVolumeType => JString(e.value)
    }

    /** Custom serializer for EbsVolumeType. To use this, add it to the default
      * formats when de-serializing...
      *
      * implicit val formats = json4s.DefaultFormats + EbsVolumeType.Serializer
      */
    case object Serializer extends CustomSerializer[EbsVolumeType](format => deserialize -> serialize)
  }
}