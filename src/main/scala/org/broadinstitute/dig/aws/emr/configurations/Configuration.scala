package org.broadinstitute.dig.aws.emr.configurations

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import software.amazon.awssdk.services.emr.model

/** Configurations are a tree-like set of properties and child configurations that
  * are used to configure an EMR cluster.
  *
  * See: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html
  */
class Configuration(val classification: String) {
  private val properties = mutable.Map.empty[String, String]
  private val configurations = mutable.Map.empty[String, Configuration]

  /** Merge another configuration into this one, overriding values. */
  def merge(configuration: Configuration): this.type = {
    require(configuration.classification == classification)

    // append the properties and child configurations
    properties ++= configuration.properties
    configurations ++ configuration.configurations

    this
  }

  /** Add a property to the configuration. */
  def addProperty(prop: (String, String)): this.type = {
    properties += prop
    this
  }

  /** Add multiple properties to the configuration. */
  def addProperties(props: Map[String, String]): this.type = {
    properties ++= props
    this
  }

  /** Add a new child configuration. */
  def addConfiguration(configuration: Configuration): this.type = {
    configurations += configuration.classification -> configuration
    this
  }

  /** Add or update an existing child configuration. */
  def withConfiguration[U <: Configuration](default: U)(f: U => U): this.type = {
    configurations.updateWith(default.classification) {
      _.map(_.asInstanceOf[U]).orElse(Some(default)).map(f)
    }

    this
  }

  /** Build the EMR configuration object. */
  def build: model.Configuration = {
    model.Configuration
      .builder
      .classification(classification)
      .configurations(configurations.valuesIterator.map(_.build).toList.asJava)
      .properties(properties.asJava)
      .build
  }
}