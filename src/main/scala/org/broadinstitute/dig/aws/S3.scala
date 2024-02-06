package org.broadinstitute.dig.aws

import com.typesafe.scalalogging.LazyLogging

import java.net.URI
import java.nio.file.{Files, NoSuchFileException, Path}

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.io.Source
import scala.util.{Failure, Success, Try}

import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

import org.broadinstitute.dig.aws.compat.Shims

object S3 extends LazyLogging {

  /** S3 client for storage. All buckets can share a single client. */
  lazy val client: S3Client = S3Client.builder.build

  /** Methods for interacting with a specific bucket. */
  final class Bucket(val bucket: String, val maybeSubdir: Option[String]) {

    /** When passing paths to jobs if there is a subdir add it to bucket name else just bucket */
    val path: String = maybeSubdir.map { subdir => s"$bucket/$subdir"}.getOrElse(bucket)

    /** To define bucket and then separate key, add subdir/ as a prefix to key, else just key */
    def keyPath(key: String): String = {
      maybeSubdir.map { subdir => s"$subdir/$key" }.getOrElse(key)
    }

    /** When a file is received strip off the prefix as well if it is to be used as inputs */
    def stripSubdir(key: String): String = {
      maybeSubdir.map { subdir => key.stripPrefix(s"$subdir/") }.getOrElse(key)
    }

    /** Returns the S3 URI of a given key. */
    def s3UriOf(key: String): URI = {
      new URI(s"s3://$bucket/${keyPath(key)}")
    }

    /** Returns the public URI of a given key. */
    def publicUriOf(key: String): URI = {
      val req = GetUrlRequest.builder.bucket(bucket).key(keyPath(key)).build
      val url = client.utilities().getUrl(req).toExternalForm

      new URI(url)
    }

    /** Test whether or not a key exists. */
    def keyExists(key: String): Boolean = {
      val req = GetObjectRequest.builder.bucket(bucket).key(keyPath(key)).range("bytes=0-0").build

      // an exception indicates that the object doesn't exist
      Try(client.getObject(req)) match {
        case Success(stream) => stream.close(); true
        case _               => false
      }
    }

    /** Get the head request of an object in the bucket. */
    def head(key: String): ResponseInputStream[GetObjectResponse] = {
      client.getObject(GetObjectRequest.builder.bucket(bucket).key(keyPath(key)).range("bytes=0-0").build)
    }

    /** Get an object in the bucket, returns a stream. */
    def get(key: String): ResponseInputStream[GetObjectResponse] = {
      client.getObject(GetObjectRequest.builder.bucket(bucket).key(keyPath(key)).build)
    }

    /** Extract the metadata for an object. */
    def getMetadata(key: String): Map[String, String] = {
      head(key).response.metadata().asScala.toMap
    }

    /** Upload a string to the bucket. */
    def put(key: String, content: String): PutObjectResponse = {
      val fixed   = content.replace("\r\n", "\n")
      val request = RequestBody.fromString(fixed)

      client.putObject(PutObjectRequest.builder.bucket(bucket).key(keyPath(key)).build, request)
    }

    /** Upload a file to the bucket. */
    def putFile(key: String, file: Path): PutObjectResponse = {
      client.putObject(PutObjectRequest.builder.bucket(bucket).key(keyPath(key)).build, RequestBody.fromFile(file))
    }

    /** Upload a resource to the bucket. */
    def putResource(key: String, resource: String): PutObjectResponse = {
      Try(Source.fromResource(resource).mkString) match {
        case Success(content) => put(key, content)
        case Failure(_)       => throw new Exception(s"Failed to read resource: $resource")
      }
    }

    /** Create a zero-byte file key in the bucket. */
    def touch(key: String): PutObjectResponse = {
      put(key, "")
    }

    /** Download the contents of a key to a file. */
    def download(key: String, dest: Path, overwrite: Boolean = false): Try[GetObjectResponse] = {
      val delete = if (overwrite) Try(Files.delete(dest)) else Success(())
      val req    = GetObjectRequest.builder.bucket(bucket).key(keyPath(key)).build

      delete match {
        case Success(_) | Failure(_: NoSuchFileException) => Try(client.getObject(req, dest))
        case Failure(ex)                                  => Failure(ex)
      }
    }

    /** List all the objects with a specific prefix recursively. */
    def ls(prefix: String): List[S3Object] = {
      val req     = ListObjectsV2Request.builder.bucket(bucket).prefix(keyPath(prefix)).build
      val it      = client.listObjectsV2Paginator(req).iterator().asScala
      val objects = new ListBuffer[S3Object]()

      // find all the keys in each object listing
      for (listing <- it) {
        import Shims._
        
        objects.addAll(listing.contents.asScala)

        // recursively follow common prefixes
        for (commonPrefix <- listing.commonPrefixes.asScala.map(_.prefix)) {
          objects.addAll(ls(commonPrefix))
        }
      }

      objects.result().map { obj =>
        obj.toBuilder.key(stripSubdir(obj.key)).build()
      }
    }

    /** Delete a key (or all keys under a prefix) from S3. */
    def rm(key: String): Unit = {
      if (key.endsWith("/")) {
        val req = ListObjectsV2Request.builder.bucket(bucket).prefix(keyPath(key)).build
        val it  = client.listObjectsV2Paginator(req).iterator().asScala

        for (listing <- it) {
          if (listing.hasContents) {
            val keys            = listing.contents.asScala.map(_.key)
            val objectsToDelete = keys.map(ObjectIdentifier.builder.key(_).build)
            val delete          = Delete.builder.objects(objectsToDelete.asJava).build
            val req             = DeleteObjectsRequest.builder.bucket(bucket).delete(delete).build

            // delete all the objects in this listing
            client.deleteObjects(req)
          }
        }
      } else {
        val req = DeleteObjectRequest.builder.bucket(bucket).key(keyPath(key)).build

        // delete a single object
        client.deleteObject(req)
        ()
      }
    }
  }
}
