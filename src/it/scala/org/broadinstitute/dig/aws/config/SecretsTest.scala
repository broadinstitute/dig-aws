package org.broadinstitute.dig.aws.config

import org.scalatest.FunSuite

final class SecretsTest extends FunSuite {
  test("secrets") {
    val secret = Secrets.get[SecretsTest.TestSecret]("hello")

    assert(secret.isSuccess)
    assert(secret.get.hello == "world")
  }
}

object SecretsTest {
  private final case class TestSecret(hello: String)
}
