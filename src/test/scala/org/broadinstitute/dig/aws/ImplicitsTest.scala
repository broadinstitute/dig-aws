package org.broadinstitute.dig.aws

import org.scalatest.FunSuite
import java.net.URI

final class ImplicitsTest extends FunSuite {
  test("uri basename") {
    import Implicits.RichURI

    assert(new URI("http://example.com/foo").basename == "foo")
    assert(new URI("http://example.com/foo/bar/baz").basename == "baz")
    assert(new URI("http://example.com/").basename == "")
    assert(new URI("http://example.com").basename == "")
  }
}
