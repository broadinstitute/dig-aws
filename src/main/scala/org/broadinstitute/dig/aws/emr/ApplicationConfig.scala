package org.broadinstitute.dig.aws.emr

import software.amazon.awssdk.services.emr.model.Configuration
import scala.jdk.CollectionConverters._

/** Each application can have various configuration settings assigned to it.
  *
  * See: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html
  */
trait ApplicationConfig[U <: ApplicationConfig[U]] {
  self: {
    def copy(classificationProperties: Seq[ClassificationProperties], properties: Seq[(String, String)]): U
  } =>
  val classification: String
  val classificationProperties: Seq[ClassificationProperties] = Seq.empty
  val properties: Seq[(String, String)] = Seq.empty

  /** Add classification properties to the configuration. */
  def withClassificationProperties(classifiedProperties: ClassificationProperties): U =
    copy(classificationProperties :+ classifiedProperties, properties)

  /** Add stand-alone properties to the configuration. */
  def withProperty(property: (String, String)): U =
    copy(classificationProperties, properties :+ property)

  /** Create the EMR Configuration for this application configuration. */
  def configuration: Configuration =
    Configuration.builder
      .classification(classification)
      .configurations(classificationProperties.map(_.configuration).asJava)
      .properties(properties.toMap.asJava)
      .build
}

/** Each application has multiple configurations that it can export.
  */
final case class ClassificationProperties(classification: String, properties: Seq[(String, String)] = Seq.empty) {
  /** Add a stand-alone property to the configuration. */
  def withProperty(property: (String, String)): ClassificationProperties =
    copy(properties = properties :+ property)

  /** Create the EMR Configuration for this application. */
  def configuration: Configuration =
    Configuration.builder
      .classification(classification)
      .properties(properties.toMap.asJava)
      .build
}
