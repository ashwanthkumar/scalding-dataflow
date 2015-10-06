import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys._
import sbtassembly.{MergeStrategy, PathList}

import scala.collection.JavaConversions._

object Build extends Build {
  val AppVersion = {
    System.getenv().getOrElse("SNAP_PIPELINE_COUNTER", "1.0.0-SNAPSHOT") match {
      // FIXME - Remove the "SNAPSHOT" suffix once we're ready to make releases
      case v if !v.endsWith("SNAPSHOT") => "1.0." + v + "-SNAPSHOT"
      case v => v
    }
  }
  val ScalaVersion = "2.10.4"

  lazy val main = Project("scalding-dataflow", file("."), settings = defaultSettings ++ publishSettings ++ assemblySettings)
    .settings(
      libraryDependencies ++= appDependencies
    )

  import Dependencies._

  lazy val appDependencies = Seq(scalding, dataflow, hadoopClient,
    scalaTest, mockito)

  lazy val defaultSettings = Seq(
    organization := "in.ashwanthkumar",
    version := AppVersion,
    fork in run := false,
    parallelExecution in This := false,

    // Custom resolvers
    resolvers += Resolver.sonatypeRepo("snapshots"),
    resolvers += "Conjars" at "http://conjars.org/repo",
    resolvers += "Cloudera" at "https://repository.cloudera.com/cloudera/public"
  )

  lazy val depsThatNeedsToBeExcluded = List(
    "hadoop.*", "zookeeper.*", "junit.*"
  )

  lazy val assemblySettings = Seq(
    mainClass in assembly := None,
    packageOptions in assembly ~= { os =>
      os filterNot (_.isInstanceOf[Package.MainClass])
    },
    assemblyExcludedJars in assembly := {
      val cp = (fullClasspath in assembly).value
      cp filter {dep =>
        depsThatNeedsToBeExcluded.exists(dep.data.getName matches)
      }
    },
    assemblyMergeStrategy in assembly := {
      case "reference.conf" | "rootdoc.txt" =>
        MergeStrategy.concat
      case PathList("META-INF", xs @ _*) =>
        xs map {
          _.toLowerCase
        } match {
          case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
            MergeStrategy.discard
          case ps@(x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
            MergeStrategy.discard
          case ("log4j.properties" :: Nil) =>
            MergeStrategy.discard
          case "plexus" :: xs =>
            MergeStrategy.discard
          case "services" :: xs =>
            MergeStrategy.filterDistinctLines
          case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
            MergeStrategy.filterDistinctLines
          case ("mailcap.default" :: Nil) | ("mimetypes.default" :: Nil) =>
            MergeStrategy.filterDistinctLines
          case ("javamail.default.providers" :: Nil) | ("mailcap" :: Nil) | ("javamail.default.address.amp" :: Nil) =>
            MergeStrategy.filterDistinctLines
          case ("javamail.charset.map" :: Nil) | ("gfprobe-provider.xml" :: Nil) =>
            MergeStrategy.filterDistinctLines
          case _ => MergeStrategy.discard
        }
      case _ => MergeStrategy.first
    }
  )

  lazy val publishSettings = xerial.sbt.Sonatype.sonatypeSettings ++ Seq(
    pomIncludeRepository := { _ => true },
    publishMavenStyle := true,
    publishArtifact in Test := false,
    publishArtifact in(Compile, packageDoc) := true,
    publishArtifact in(Compile, packageSrc) := true,
    publishTo <<= version { (v: String) =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
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

  val dataflow = "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % "1.1.0"
  val scalding = "com.twitter" % "scalding-core_2.10" % "0.15.0"
  val hadoopClient = "org.apache.hadoop" % "hadoop-client" % "2.6.0-mr1-cdh5.4.4" % "provided"
  val scalaTest = "org.scalatest" %% "scalatest" % "2.2.0" % "test"
  val mockito = "org.mockito" % "mockito-all" % "1.9.5" % "test"

}
