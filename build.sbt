name := "hbase-spark-query-language"

organization := "com.github.davechendatascience"

version := "0.1.0"

scalaVersion := "2.12.18"

// Dependency versions
val sparkVersion = "3.5.1"
val hadoopVersion = "3.3.6"
val hbaseVersion = "2.5.8"
val hbaseConnectorsVersion = "1.0.0"
val json4sVersion = "3.7.0-M11"
val typesafeConfigVersion = "1.4.3"
val scalaTestVersion = "3.2.18"

libraryDependencies ++= Seq(
  // Spark 3.5.x
  "org.apache.spark"  %% "spark-core"    % sparkVersion % "provided",
  "org.apache.spark"  %% "spark-sql"     % sparkVersion % "provided",
  "org.apache.spark"  %% "spark-mllib"   % sparkVersion % "provided",

  // Apache HBase 2.5.x
  "org.apache.hbase"   % "hbase-client"              % hbaseVersion % "provided",
  "org.apache.hbase"   % "hbase-common"              % hbaseVersion % "provided",
  "org.apache.hbase"   % "hbase-server"              % hbaseVersion % "provided",
  "org.apache.hbase"   % "hbase-mapreduce"           % hbaseVersion % "provided",
  "org.apache.hbase"   % "hbase-shaded-client"       % hbaseVersion % "provided",
  "org.apache.hbase"   % "hbase-shaded-mapreduce"    % hbaseVersion % "provided",

  // Apache HBase Connectors for Spark 3
  "org.apache.hbase.connectors.spark" % "hbase-spark3" % hbaseConnectorsVersion % "provided",

  // JSON & Utilities
  "org.json4s"        %% "json4s-jackson" % json4sVersion,
  "com.typesafe"       % "config"         % typesafeConfigVersion,

  // Testing
  "org.scalatest"     %% "scalatest"      % scalaTestVersion % "test"
)

resolvers ++= Seq(
  "Apache Releases" at "https://repository.apache.org/content/repositories/releases/",
  "Apache Snapshots" at "https://repository.apache.org/content/repositories/snapshots/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  Resolver.mavenCentral,
  Resolver.sonatypeRepo("public")
)

// Modern sbt-assembly 2.x merge strategy
ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) =>
    xs match {
      case "MANIFEST.MF" :: Nil => MergeStrategy.discard
      case "services" :: _      => MergeStrategy.concat
      case _                    => MergeStrategy.discard
    }
  case PathList("javax", "servlet", _ @ _*) => MergeStrategy.first
  case PathList("org", "apache", _ @ _*)    => MergeStrategy.first
  case PathList("org", "slf4j", _ @ _*)     => MergeStrategy.first
  case "reference.conf"                     => MergeStrategy.concat
  case "application.conf"                   => MergeStrategy.concat
  case "about.html"                         => MergeStrategy.rename
  case _                                    => MergeStrategy.first
}
