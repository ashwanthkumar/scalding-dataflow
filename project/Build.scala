import sbt._
import sbt.Keys._
import scala.collection.JavaConversions._

object Build extends Build {
  val AppVersion = System.getenv().getOrElse("GO_PIPELINE_LABEL", "1.0.0-SNAPSHOT")
  val ScalaVersion = "2.10.4"

  val main = Project("scalding-dataflow", file("."))
    .settings(organization := "in.ashwanthkumar",
      version := AppVersion,
      libraryDependencies ++= appDependencies
    )

  import Dependencies._

  lazy val appDependencies = Seq(scalding, dataflow, hadoopClient,
    scalaTest, mockito)

  override val settings = super.settings ++ Seq(
    fork in run := false,
    parallelExecution in This := false,

    // Custom resolvers
    resolvers += Resolver.sonatypeRepo("snapshots"),
    resolvers += "Conjars" at "http://conjars.org/repo",
    resolvers += "Cloudera" at "https://repository.cloudera.com/cloudera/public"
  )

  lazy val publishSettings = xerial.sbt.Sonatype.sonatypeSettings ++ Seq(
    mappings in (Compile, packageBin) ~= (_.filterNot{case (file, _) => file.isDirectory && file.getName == "kinglear.txt"}),
    publishMavenStyle := true,
    publishArtifact in Test := false,
    publishArtifact in(Compile, packageDoc) := true,
    publishArtifact in(Compile, packageSrc) := true,
    publishTo <<= version { (v: String) =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    },
    pomExtra := _pomExtra
  )

  val _pomExtra =
    <url>http://github.com/ashwanthkumar/scalding-dataflow</url>
      <licenses>
        <license>
          <name>Apache License, Version 2.0</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:ashwanthkumar/scalding-dataflow.git</url>
        <connection>scm:git:git@github.com:ashwanthkumar/scalding-dataflow.git</connection>
      </scm>
      <developers>
        <developer>
          <id>ashwanthkumar</id>
          <name>Ashwanth Kumar</name>
          <url>http://www.ashwanthkumar.in/</url>
        </developer>
      </developers>

}

object Dependencies {

  val scalding = "com.twitter" % "scalding-core_2.10" % "0.14.0"
  val dataflow = "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % "1.1.0"
  val hadoopClient = "org.apache.hadoop" % "hadoop-client" % "2.6.0-mr1-cdh5.4.4"
  val scalaTest = "org.scalatest" %% "scalatest" % "2.2.0" % "test"
  val mockito = "org.mockito" % "mockito-all" % "1.9.5" % "test"

}
