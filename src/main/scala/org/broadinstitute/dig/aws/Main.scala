package org.broadinstitute.dig.aws

object Main extends App {
  val versions = Versions.load().get
  
  println(versions)
}
