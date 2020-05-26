package org.broadinstitute.dig.aws.config

import scala.util.Try

/** RDS configuration settings. */
final case class RdsConfig(instance: String) {
  def secret: Try[RdsConfig.Secret] = Secrets.get[RdsConfig.Secret](instance)
}

/** Companion object. */
object RdsConfig {

  /** RDS connection settings retrieved from the secrets manager. */
  final case class Secret(
    host: String,
    port: Int,
    engine: String,
    username: String,
    password: String,
    dbname: String,
  ) {

    /** Query parameters to the connection string URL. */
    private val qs = List("useCursorFetch" -> true, "useSSL" -> false)
      .map(p => s"${p._1}=${p._2}")
      .mkString("&")

    /** The connection string to use for a specific schema. */
    def connectionString(schema: String): String = s"jdbc:$engine://$host:$port/$schema?$qs"

    /** The connection string for the default schema. */
    def connectionString: String = connectionString(dbname)
  }
}