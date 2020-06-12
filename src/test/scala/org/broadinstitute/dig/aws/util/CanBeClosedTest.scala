package org.broadinstitute.dig.aws.util

import org.scalatest.FunSuite

/**
 *  @author  kyuksel
 *  Aug 30, 2017
 *  @author clint
 *  Jun 9, 2020
 */
final class CanBeClosedTest extends FunSuite {
  import CanBeClosedTest._

  test("using() closes things properly") {
    val foo = new Foo

    assert(foo.isClosed === false)

    val result = CanBeClosed.using(foo) { _ =>
      42
    }

    assert(result === 42)
    assert(foo.isClosed === true)
  }

  test("using() can handle exceptions") {
    val foo = new Foo

    assert(foo.isClosed === false)

    intercept[Exception] {
      CanBeClosed.using(foo) { _ =>
        throw new Exception("something went wrong")
      }
    }

    assert(foo.isClosed === true)
  }
}

object CanBeClosedTest {
  private final class Foo extends java.io.Closeable {
    var isClosed = false

    override def close(): Unit = isClosed = true
  }
}

