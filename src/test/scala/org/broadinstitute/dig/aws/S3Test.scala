package org.broadinstitute.dig.aws

import java.net.URI

import org.scalatest.FunSuite

/**
 * @author clint
 * Jun 9, 2020
 */
final class S3Test extends FunSuite {
  test("s3UriOf") {
    val expected = new URI(s"s3://lalala/foo")

    val actual = S3.Bucket("lalala").s3UriOf("foo")

    assert(expected === actual)
  }
}
