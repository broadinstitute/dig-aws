package org.broadinstitute.dig.aws.emr

import com.amazonaws.services.elasticmapreduce.model.Configuration
import scala.collection.JavaConverters._

/** Each application can have various configuration settings assigned to it.
  *
  * See: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html
  */
final case class ApplicationConfig(classification: String, configs: ClassificationProperties*) {

  /** Create a new App with additional configuration properties. */
  def withConfig(config: ClassificationProperties): ApplicationConfig = {
    ApplicationConfig(classification, configs :+ config: _*)
  }

  /** Create the EMR Configuration for this application. */
  def configuration: Configuration =
    new Configuration()
      .withClassification(classification)
      .withConfigurations(configs.map(_.configuration).asJava)
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
    new Configuration()
      .withClassification(classification)
      .withProperties(properties.toMap.asJava)
}

/** Companion object containing some typical configurations.
  */
object ApplicationConfig {

  /** Some common configurations that can be extended. */
  val sparkDefaults = ApplicationConfig("spark-defaults")
  val sparkEnv      = ApplicationConfig("spark-env")
}

/** Companion object with typical properties.
  */
object ClassificationProperties {

  /** Python3 spark configuration setting. */
  val sparkUsePython3 = ClassificationProperties("export", "PYSPARK_PYTHON" -> "/usr/bin/python3")
}
