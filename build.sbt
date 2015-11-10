/********************
 * Identity         *
 ********************/
name := "cookie-datasets"
organization := "ai.cookie"
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

/********************
 * Version          *
 ********************/
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.10.4"
sparkVersion := "1.5.0"
//scalatestVersion := "2.2.5"

/********************
  * scaladocs *
  ********************/
autoAPIMappings := true

/********************
 * Release settings *
 ********************/
spAppendScalaVersion := true
spIncludeMaven := true
spIgnoreProvided := true
publishMavenStyle := true
releaseCrossBuild := true
releasePublishArtifactsAction := PgpKeys.publishSigned.value
pomExtra :=
  <url>https://github.com/cookieai/cookie-datasets</url>
  <scm>
    <url>git@github.com:cookieai/cookie-datasets.git</url>
    <connection>scm:git:git@github.com:cookieai/cookie-datasets.git</connection>
  </scm>
  <developers>
    <developer>
      <id>EronWright</id>
      <name>Eron Wright</name>
      <url>https://github.com/EronWright</url>
    </developer>
  </developers>

bintrayReleaseOnPublish in ThisBuild := false

import ReleaseTransformations._

// Add publishing to spark packages as another step.
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  pushChanges,
  releaseStepTask(spPublish)
)


/********************
 * Dependencies     *
 ********************/
sparkComponents := Seq("sql", "mllib")
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion.value,
  "org.apache.spark" %% "spark-mllib" % sparkVersion.value
)
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.5" % Test,
  "org.apache.spark" %% "spark-core" % sparkVersion.value % Test classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % Test classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % Test classifier "tests"
)