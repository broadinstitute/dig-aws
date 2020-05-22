lazy val Versions = new {
  val Aws              = "2.11.12"
  val Janino           = "3.1.2"
  val LogbackClassic   = "1.2.3"
  val LogbackColorizer = "1.0.1"
  val Scala            = "2.13.2"
  val ScalaLogging     = "3.9.2"
  val ScalaTest        = "3.0.8"
  val Json4s           = "3.6.8"
  val TypesafeConfig   = "1.3.4"
  val Ficus            = "1.4.7"
}

lazy val Orgs = new {
  val DIG = "org.broadinstitute.dig"
}

lazy val scalacOpts = Seq(
  "-feature",
  "-deprecation",
  "-unchecked",
  "-Ywarn-value-discard"
)

lazy val mainDeps = Seq(
  "software.amazon.awssdk"         % "emr"                  % Versions.Aws,
  "software.amazon.awssdk"         % "s3"                   % Versions.Aws,
  "software.amazon.awssdk"         % "ec2"                  % Versions.Aws,
  "software.amazon.awssdk"         % "secretsmanager"       % Versions.Aws,
  "com.typesafe.scala-logging"     %% "scala-logging"       % Versions.ScalaLogging,
  "ch.qos.logback"                 % "logback-classic"      % Versions.LogbackClassic,
  "org.codehaus.janino"            % "janino"               % Versions.Janino,
  "org.tuxdude.logback.extensions" % "logback-colorizer"    % Versions.LogbackColorizer,
  "org.json4s"                     %% "json4s-jackson"      % Versions.Json4s,
  "com.typesafe"                   % "config"               % Versions.TypesafeConfig,
  "com.iheart"                     %% "ficus"               % Versions.Ficus,
)

lazy val testDeps = Seq(
  "org.scalatest"  %% "scalatest" % Versions.ScalaTest % "it,test"
)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings: _*)
  .settings(
    name := "dig-aws",
    organization := Orgs.DIG,
    //NB: version set in version.sbt
    scalaVersion := Versions.Scala,
    scalacOptions ++= scalacOpts,
    libraryDependencies ++= (mainDeps ++ testDeps)
  )

//Make integration tests run serially.
parallelExecution in IntegrationTest := false

//Show full stack traces from unit and integration tests (F); display test run times (D)
testOptions in IntegrationTest += Tests.Argument("-oFD")
testOptions in Test += Tests.Argument("-oFD")

//Enables `buildInfoTask`, which bakes git version info into the LS jar.
enablePlugins(GitVersioning)

val buildInfoTask = taskKey[Seq[File]]("buildInfo")

buildInfoTask := {
  val dir                   = (resourceManaged in Compile).value
  val n                     = name.value
  val v                     = version.value
  val branch                = git.gitCurrentBranch.value
  val lastCommit            = git.gitHeadCommit.value
  val describedVersion      = git.gitDescribedVersion.value
  val anyUncommittedChanges = git.gitUncommittedChanges.value
  val remoteUrl             = (scmInfo in ThisBuild).value.map(_.browseUrl.toString)

  val buildDate = java.time.Instant.now

  val file = dir / s"versionInfo_${n}.properties"

  val log = streams.value.log

  log.info(s"Writing version info to '$file'")

  val contents =
    s"""|name=${n}
        |version=${v}
        |branch=${branch}
        |lastCommit=${lastCommit.getOrElse("")}
        |uncommittedChanges=${anyUncommittedChanges}
        |describedVersion=${describedVersion.getOrElse("")}
        |buildDate=${buildDate}
        |remoteUrl=${remoteUrl.getOrElse("")}
        |""".stripMargin

  IO.write(file, contents)

  Seq(file)
}

(resourceGenerators in Compile) += buildInfoTask.taskValue

import ReleaseTransformations._

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies, // : ReleaseStep
  inquireVersions,           // : ReleaseStep
  runClean,                  // : ReleaseStep
  runTest,                   // : ReleaseStep
  setReleaseVersion,         // : ReleaseStep
  commitReleaseVersion,      // : ReleaseStep, performs the initial git checks
  tagRelease,                // : ReleaseStep
  // run 'publishLocal' instead of 'publish', since publishing to a repo on the Broad FS never resulted in
  // artifacts that could be resolved by other builds. :(
  // See: https://github.com/sbt/sbt-release#can-we-finally-customize-that-release-process-please
  //      https://stackoverflow.com/questions/44058275/add-docker-publish-step-to-sbt-release-process-with-new-tag
  //      https://github.com/sbt/sbt/issues/1917
  releaseStepCommand("publishLocal"),
  setNextVersion,    // : ReleaseStep
  commitNextVersion, // : ReleaseStep
  pushChanges        // : ReleaseStep, also checks that an upstream branch is properly configured
)
