package org.broadinstitute.dig.aws

import org.scalatest.FunSuite


/**
 * @author clint
 * Sep 3, 2019
 */
final class ThrowablesTest extends FunSuite {
  test("quietly") {
    import Throwables.quietly
    
    val dontLog: (String, Throwable) => Unit = (_, _) => ()
    
    assert(quietly("foo", dontLog)(42) === None)
    
    var logFnParams: Seq[(String, Throwable)] = Vector.empty
    
    val myLogFn: (String, Throwable) => Unit = (msg, t) => {
      logFnParams :+= (msg -> t)
      
      ()
    }
    
    assert(logFnParams.isEmpty)
    
    assert(quietly("foo", myLogFn)(42) === None)
    
    assert(logFnParams.isEmpty)
    
    val e = new Exception with scala.util.control.NoStackTrace
    
    assert(quietly("blarg", myLogFn)(throw e) === Some(e))
    
    assert(logFnParams === Seq(("blarg", e)))
  }
}
