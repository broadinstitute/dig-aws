package org.broadinstitute.dig.aws

import java.net.URI
import java.nio.file.Paths

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.concurrent.ExecutionContext
import scala.io.Source
import scala.util.Try

import cats.effect.ContextShift
import cats.effect.IO
import cats.effect.Timer
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.services.emr.model.StepState
import software.amazon.awssdk.services.emr.model.StepSummary
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable

object Implicits {

  object Defaults {
    /** Needed for IO.sleep. */
    implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

    /** Needed for IO.parSequence. */
    implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  }

  /** Helper functions for S3 objects. */
  final implicit class RichResponseInputStream[A](val responseInputStream: ResponseInputStream[A]) extends AnyVal {

    /** Read the entire contents of an S3 object as a string, sonsuming the ResponseInputStream 
     *  and disposing of it afterward. */ 
    def readAsString(): String = {
      try {
        Source.fromInputStream(responseInputStream).mkString
      } finally {
        responseInputStream.close()
      }
    }
  }

  /** Helper functions for common S3 operations that are a little tricky. */
  final implicit class RichS3Client(val s3: S3Client) extends AnyVal {

    /** Test whether or not a key exists. */
    def keyExists(bucket: String, key: String): Boolean = {
      val req = GetObjectRequest.builder
                .bucket(bucket)
                .key(key)
                // ask for as little data as possible
                .range("bytes=0-0")
                .build

      // an exception indicates that the object doesn't exist
      Try(s3.getObject(req))
        .map { responseStream =>
          responseStream.close()

          true
        }.getOrElse(false)
    }

    /** List all the keys at a given location. */
    def listingsIterable(bucket: String, key: String): ListObjectsV2Iterable = {
      val request = ListObjectsV2Request.builder.bucket(bucket).prefix(key).build
      
      s3.listObjectsV2Paginator(request)
    }

    /** Collect all the keys (recursively) into a collection. */
    def listKeys(bucket: String, key: String, recursive: Boolean = true): Seq[String] = {
      val responses = listingsIterable(bucket, key).iterator.asScala
      val keys: Buffer[String] = new ArrayBuffer

      // find all the keys in each object listing
      for (response <- responses) {
        keys ++= response.keys

        if (recursive) {
          for (commonPrefix <- response.commonPrefixNames) {
            keys ++= listKeys(bucket, commonPrefix, recursive)
          }
        }
      }

      keys.toSeq
    }
  }

  /** RichS3Client.listKeys returns one of these... */
  final implicit class RichListObjectsV2Response(val listing: ListObjectsV2Response) extends AnyVal {

    //TODO: Is this how to tell if the listing is empty?
    def isEmpty: Boolean = listing.contents.isEmpty && listing.commonPrefixes.isEmpty
    
    /** Extract all the object keys from an object listing iterator. */
    def keys: Seq[String] = listing.contents.iterator.asScala.map(_.key).toList

    /** Extract all the common prefixes. */
    def commonPrefixNames: Seq[String] = listing.commonPrefixes.iterator.asScala.map(_.prefix).toList
  }

  /**
   * When dealing with S3 paths, it's often helpful to be able to get
   * just the final filename from a path.
   * Will return the empty string if the URI doesn't have a path part, or if the path part is empty.
   */
  final implicit class RichURI(val uri: URI) extends AnyVal {
    def basename: String = {
      val path = Paths.get(uri.getPath)

      // extract just the final part of the path
      path.iterator.asScala.toSeq.lastOption.map(_.toString).getOrElse("")
    }
  }

  /** Helper functions for a Hadoop job step. */
  final implicit class RichStepSummary(val summary: StepSummary) extends AnyVal {
    def state: StepState = summary.status.state

    /** True if this step matches another step and the state hasn't changed. */
    def matches(step: StepSummary): Boolean = {
      summary.id == step.id && summary.state == step.state
    }

    /** True if this step has successfully completed. */
    def isComplete: Boolean = state == StepState.COMPLETED

    /** True if this step failed. */
    def isFailure: Boolean = state == StepState.FAILED

    /** True if the step hasn't started yet. */
    def isPending: Boolean = state == StepState.PENDING

    /** True if the step is actively running. */
    def isRunning: Boolean = state == StepState.RUNNING

    /** If failed, this is the reason why. */
    def failureReason: Option[String] = {
      Option(summary.status).map(_.failureDetails).flatMap { details =>
        Option(details).flatMap(d => Option(d.message))
      }
    }

    /** True if this step stopped for any reason. */
    def isStopped: Boolean = state match {
      case StepState.FAILED =>      true
      case StepState.INTERRUPTED => true
      case StepState.CANCELLED =>   true
      case _ =>                     false
    }

    /** Return a reason for why this step was stopped. */
    def stopReason: String = {
      failureReason.getOrElse {
        state match {
          case StepState.INTERRUPTED => state.toString
          case StepState.CANCELLED =>   state.toString
          case _ =>                     "Unknown"
        }
      }
    }
  }
}
