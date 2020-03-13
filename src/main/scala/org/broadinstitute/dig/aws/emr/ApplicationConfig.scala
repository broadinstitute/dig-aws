package org.broadinstitute.dig.aws.emr

import software.amazon.awssdk.services.emr.model.Configuration
import scala.collection.JavaConverters._

/** Each application can have various configuration settings assigned to it.
  *
  * See: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html
  */
final case class ApplicationConfig(
    classification: String,
    configs: Seq[ClassificationProperties] = Seq.empty,
    props: Seq[(String, String)] = Seq.empty) {

  /** Create a new App with additional configuration properties. */
  def withConfig(newConfigs: ClassificationProperties*): ApplicationConfig = copy(configs = this.configs ++ newConfigs)

  /** Add a property to this application configuration. */
  def withProperty(newProps: (String, String)*): ApplicationConfig = copy(props = this.props ++ newProps)

  /** Create the EMR Configuration for this application. */
  def configuration: Configuration =
    Configuration.builder
      .classification(classification)
      .configurations(configs.map(_.configuration).asJava)
      .properties(props.toMap.asJava)
      .build
}

/** Each application has multiple configurations that it can export.
  */
final case class ClassificationProperties(classification: String, properties: (String, String)*) {

  /** Create a new classification with additional properties. */
  def withProperties(props: (String, String)*): ClassificationProperties = {
    ClassificationProperties(classification, properties ++ props: _*)
  }

  /** Create the EMR Configuration for this application. */
  def configuration: Configuration =
    Configuration.builder
      .classification(classification)
      .properties(properties.toMap.asJava)
      .build
}

/** Companion object containing some typical configurations.
  */
object ApplicationConfig {

  /** Some common configurations that can be extended. */
  val sparkDefaults: ApplicationConfig = ApplicationConfig("spark-defaults")
  val sparkEnv: ApplicationConfig      = ApplicationConfig("spark-env")

  /** A common configuration for Spark */
  var sparkMaximizeResourceAllocation: ApplicationConfig = ApplicationConfig("spark")
    .withProperty("maximizeResourceAllocation" -> "true")
}

/** Companion object with typical properties.
  */
object ClassificationProperties {

  /** Python3 spark configuration setting. */
  val sparkUsePython3: ClassificationProperties = ClassificationProperties("export", "PYSPARK_PYTHON" -> "/usr/bin/python3")
}
