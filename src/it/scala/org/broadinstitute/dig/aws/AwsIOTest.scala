package org.broadinstitute.dig.aws

import cats._
import cats.effect._
import cats.implicits._

import java.nio.charset.Charset

import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import org.json4s.Formats
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read
import org.broadinstitute.dig.aws.config.emr.EmrConfig
import org.broadinstitute.dig.aws.config.AWSConfig
import scala.io.Source
import java.io.File
import software.amazon.awssdk.services.s3.model.S3Object
import software.amazon.awssdk.core.ResponseInputStream

/**
 * @author clint
 * Jul 27, 2018
 */
final class AwsIOTest extends AwsTest[IO]
