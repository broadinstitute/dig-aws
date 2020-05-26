package org.broadinstitute.dig.aws

import java.net.URI

import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.services.s3.model.S3Object

import scala.io.Source

object Implicits {

  /** URI methods. */
  final implicit class RichURI(val uri: URI) extends AnyVal {

    /** Final "filename" of the resource. */
    def basename: String = {
      val path = uri.getPath
      val lastSep = path.indexOf('/')

      path.substring(lastSep + 1)
    }
  }

  /** S3 object methods. */
  final implicit class RichS3Object(val obj: S3Object) extends AnyVal {

    /** S3Object has quoted E-Tags due to historical (but wrong) reasons... */
    def eTag: String = {
      val first = obj.eTag.indexOf('"')

      obj.eTag.lastIndexOf('"') match {
        case n if n < 0 => obj.eTag.substring(first + 1)
        case n          => obj.eTag.substring(first + 1, n)
      }
    }
  }

  /** S3 response stream methods. */
  final implicit class RichResponseInputStream[A](val stream: ResponseInputStream[A]) extends AnyVal {

    /** Read contents of a response as a stream, then close the stream. */
    def mkString(): String = {
      try {
        Source.fromInputStream(stream).mkString
      }
      finally {
        stream.close()
      }
    }
  }
}
