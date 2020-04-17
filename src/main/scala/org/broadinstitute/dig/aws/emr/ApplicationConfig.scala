package org.broadinstitute.dig.aws.emr

import software.amazon.awssdk.services.emr.model.Configuration
import scala.collection.JavaConverters._

/** Each application can have various configuration settings assigned to it.
  *
  * See: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html
  */
case class ApplicationConfig(
    classification: String,
    classifiedProperties: Seq[ClassificationProperties] = Seq.empty,
    properties: Seq[ApplicationConfig.Property] = Seq.empty,
) {
  /** Create the EMR Configuration for this application configuration. */
  def configuration: Configuration =
    Configuration.builder
      .classification(classification)
      .configurations(classifiedProperties.map(_.configuration).asJava)
      .properties(properties.toMap.asJava)
      .build

  /** Create a new ApplicationConfig with additional configuration properties. */
  def withClassifiedProperties(newClassification: ClassificationProperties): ApplicationConfig = {
    copy(classifiedProperties = classifiedProperties :+ newClassification)
  }

  /** Create a new ApplicationConfig with the specific property added. */
  def withProperty(property: ApplicationConfig.Property): ApplicationConfig = {
    copy(properties = properties :+ property)
  }
}

/** Companion object and helper types.
  */
object ApplicationConfig {
  type Property = (String, String)
}

/** Each application has multiple configurations that it can export.
  */
final case class ClassificationProperties(classification: String, properties: ApplicationConfig.Property*) {

  /** Create the EMR Configuration for this application. */
  def configuration: Configuration =
    Configuration.builder
      .classification(classification)
      .properties(properties.toMap.asJava)
      .build

  /** Create a new classification with additional properties. */
  def withProperty(property: ApplicationConfig.Property): ClassificationProperties = {
    ClassificationProperties(classification, properties.toSeq :+ property: _*)
  }
}
