package org.broadinstitute.dig.aws

import scala.util.control.NonFatal

object Throwables {
  def quietly(
      message: String, 
      doLog: (String, Throwable) => Unit)
      (f: => Any): Option[Throwable] = {
    
    try { 
      f 
      
      None
    } catch { 
      case NonFatal(e) => {
        doLog(message, e)
        
        Some(e)
      }
    }
  }
}
