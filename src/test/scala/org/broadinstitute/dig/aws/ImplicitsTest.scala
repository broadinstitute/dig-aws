package org.broadinstitute.dig.aws

import org.scalatest.FunSuite
import java.net.URI
import org.scalactic.source.Position.apply

/**
 * @author clint
 * Aug 21, 2018
 */
final class ImplicitsTest extends FunSuite {
  test("RichURI.basename") {
    import Implicits.RichURI
    
    val hasSimplePath = new URI("http://example.com/foo")
    
    assert(hasSimplePath.basename == "foo")
    
    val hasPath = new URI("http://example.com/foo/bar/baz")
    
    assert(hasPath.basename == "baz")
    
    val emptyPath = new URI("http://example.com/")
    
    assert(emptyPath.basename == "")
    
    val noPath = new URI("http://example.com")
    
    assert(noPath.basename == "")
  }
}
