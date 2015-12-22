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
crossScalaVersions := Seq("2.10.4", "2.11.7")

/********************
 * scaladocs *
 ********************/
autoAPIMappings := true

/********************
 * Test *
 ********************/
parallelExecution in Test := false
fork := true
test in assembly := {}

/*******************
 * Spark Packages
 ********************/
spName := "cookieai/cookie-datasets"
spAppendScalaVersion := true
spIncludeMaven := true
spIgnoreProvided := true

/********************
 * Release settings *
 ********************/
publishMavenStyle := true
pomIncludeRepository := { _ => false }
publishArtifact in Test := false
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}
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

/********************
 * sbt-release      *
 ********************/
// releaseCrossBuild := true
// releasePublishArtifactsAction := PgpKeys.publishSigned.value

/********************
 * Dependencies     *
 ********************/
sparkComponents := Seq("core", "sql", "mllib")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion.value % Test force(),
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % Test force(),
  "org.apache.spark" %% "spark-mllib" % sparkVersion.value % Test force(),
  "org.scalatest" %% "scalatest" % "2.2.5" % Test,
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion.value}_0.2.1" % Test
)
