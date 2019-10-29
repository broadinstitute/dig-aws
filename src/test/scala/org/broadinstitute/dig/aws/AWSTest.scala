package org.broadinstitute.dig.aws

import org.scalatest.FunSuite
import java.net.URI

/**
 * @author clint
 * Oct 29, 2019
 */
final class AWSTest extends FunSuite {
  test("keyOf") {
    val uri = URI.create("s3://some-bucket/foo/bar/baz")
    
    assert(AWS.keyOf(uri) === "/foo/bar/baz")
    
    val noPath = URI.create("s3://some-bucket")
    
    assert(AWS.keyOf(noPath) === "")
  }
  
  test("bucketOf") {
    val uri = URI.create("s3://some-bucket/foo/bar/baz")
    
    assert(AWS.bucketOf(uri) === "some-bucket")
    
    val noPath = URI.create("s3://some-bucket")
    
    assert(AWS.bucketOf(noPath) === "some-bucket")
  }
}
