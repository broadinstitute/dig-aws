package org.broadinstitute.dig.aws.config

import java.util.Base64

import scala.util.Try

import org.json4s.DefaultFormats
import org.json4s.Formats
import org.json4s.jackson.Serialization.read

import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient

object Secrets {

  /** Secrets manager.
    */
  private val client: SecretsManagerClient = SecretsManagerClient.builder.build

  /** Fetch a secret as JSON and decode it.
    */
  def get[A](secretId: String)(implicit m: Manifest[A]): Try[A] = {
    implicit val formats: Formats = DefaultFormats

    // create the request
    val req = GetSecretValueRequest.builder.secretId(secretId).build

    // fetch the secret and attempt to parse it
    Try(client.getSecretValue(req)).map { resp =>
      val secret = if (resp.secretString != null) {
        resp.secretString
      } else {
        new String(Base64.getDecoder.decode(resp.secretBinary.asByteArray))
      }

      read[A](secret)
    }
  }
}
