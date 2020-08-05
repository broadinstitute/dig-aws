package org.broadinstitute.dig.aws

import com.typesafe.scalalogging.LazyLogging

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.core.exception.SdkException

/** Useful functions shared by all can go here. */
object Utils extends LazyLogging {

  /** Very simple retry with delay and backoff for retry-able AWS calls. */
  @tailrec
  def awsRetry[T](delay: FiniteDuration=1.minute, retries: Int = 10)(body: => T): T = {
    Try(body) match {
      case Success(x)                  => x
      case Failure(ex) if retries == 0 => throw ex

      // some classes of AWS exceptions can be retried
      case Failure(ex) =>
        ex match {
          case ex : SdkException if ex.retryable()                  => ()
          case ex : AwsServiceException if ex.isThrottlingException => ()
          case _                                                    => throw ex
        }

        // warn about throttling and wait
        logger.warn(s"AWS exception: $ex; retrying in $delay")
        Thread.sleep(delay.toMillis)

        // retry, with a longer delay if it fails again
        awsRetry(delay + 1.minute, retries - 1)(body)
    }
  }
}
