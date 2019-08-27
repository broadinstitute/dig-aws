package org.broadinstitute.dig.aws

import scala.util.Try
import java.util.Properties
import java.time.Instant
import java.io.Reader
import java.io.InputStreamReader
import scala.util.Failure
import scala.util.Success

/** Based on the Versions class from LoamStream, created Oct 28, 2016. */
final case class Versions(
    name: String,
    version: String,
    branch: String,
    lastCommit: Option[String],
    anyUncommittedChanges: Boolean,
    describedVersion: Option[String],
    buildDate: Instant,
    remoteUrl: Option[String]
) {

  /** Convert to a human-readable string. */
  override def toString: String = {
    val isDirtyPart          = if (anyUncommittedChanges) " (PLUS uncommitted changes!) " else " "
    val branchPart           = s"branch: $branch"
    val describedVersionPart = describedVersion.getOrElse("UNKNOWN")
    val commitPart           = s"commit: ${lastCommit.getOrElse("UNKNOWN")}"
    val buildDatePart        = s"built on: $buildDate "
    val remoteUrlPart        = s"from ${remoteUrl.getOrElse("UNKNOWN origin")}"

    s"$name $version ($describedVersionPart) $branchPart ${commitPart}${isDirtyPart}${buildDatePart}${remoteUrlPart}"
  }
}

object Versions {

  val propsFileName: String = "versionInfo_dig-aws.properties"

  private[aws] def propsFrom(propsFile: String): Try[Properties] = {
    val propStreamOption  = Option(getClass.getClassLoader.getResourceAsStream(propsFile))
    val propStreamAttempt = toTry(propStreamOption)(s"Couldn't find '$propsFile' on the classpath")

    for {
      propStream <- propStreamAttempt
      reader     <- Try(new InputStreamReader(propStream))
      props      <- toProps(reader)
    } yield props
  }

  def load(): Try[Versions] = {
    for {
      props    <- propsFrom(propsFileName)
      versions <- loadFrom(props)
    } yield {
      versions
    }
  }

  private[aws] def loadFrom(reader: Reader): Try[Versions] = toProps(reader).flatMap(loadFrom)

  private[aws] def loadFrom(props: Properties): Try[Versions] = {
    import Implicits._

    for {
      name    <- props.tryGetProperty("name")
      version <- props.tryGetProperty("version")
      branch  <- props.tryGetProperty("branch")
      lastCommit = props.tryGetProperty("lastCommit").toOption
      anyUncommittedChanges <- props.tryGetProperty("uncommittedChanges").map(_.toBoolean)
      describedVersion = props.tryGetProperty("describedVersion").toOption
      buildDate <- props.tryGetProperty("buildDate").map(Instant.parse)
      remoteUrl = props.tryGetProperty("remoteUrl").toOption
    } yield {
      Versions(name, version, branch, lastCommit, anyUncommittedChanges, describedVersion, buildDate, remoteUrl)
    }
  }

  object Implicits {
    final implicit class PropertiesOps(val props: Properties) extends AnyVal {
      def tryGetProperty(key: String): Try[String] = {
        toTry(Option(props.getProperty(key)).map(_.trim).filter(_.nonEmpty)) {
          import scala.collection.JavaConverters._

          val sortedPropKvPairs = props.asScala.toSeq.sortBy { case (k, _) => k }

          s"property key '$key' not found in $sortedPropKvPairs"
        }
      }
    }
  }

  private def toTry[A](o: Option[A])(messageIfNone: => String): Try[A] = o match {
    case Some(a) => Success(a)
    case None    => Failure(new Exception(messageIfNone))
  }

  private[aws] def toProps(reader: Reader): Try[Properties] = Try {
    try {
      val props = new Properties

      props.load(reader)

      props
    } finally {
      reader.close()
    }
  }
}
