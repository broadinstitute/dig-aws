package org.broadinstitute.dig.aws.emr.configurations

/** Export classifications, properties, and configurations.
  */
trait Export[U <: Configuration with Export[U]] { self: U =>

  /** Add a single property to the export configuration. */
  def export(property: (String, String)): self.type = {
    withConfiguration(new Configuration("export"))(_.addProperty(property))
  }

  /** Add a multiple properties to the export configuration. */
  def export(env: Map[String, String]): self.type = {
    withConfiguration(new Configuration("export"))(_.addProperties(env))
  }
}
