package org.broadinstitute.dig.aws

import org.scalatest.FunSuite
import java.net.URI

/**
 * @author clint
 * Oct 29, 2019
 */
final class AwsTest extends FunSuite {
  test("keyOf") {
    val uri = URI.create("s3://some-bucket/foo/bar/baz")
    
    assert(Aws.keyOf(uri) === "/foo/bar/baz")
    
    val noPath = URI.create("s3://some-bucket")
    
    assert(Aws.keyOf(noPath) === "")
  }
  
  test("bucketOf") {
    val uri = URI.create("s3://some-bucket/foo/bar/baz")
    
    assert(Aws.bucketOf(uri) === "some-bucket")
    
    val noPath = URI.create("s3://some-bucket")
    
    assert(Aws.bucketOf(noPath) === "some-bucket")
  }
}
