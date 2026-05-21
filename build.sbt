ThisBuild / scalacOptions ++= Seq("-deprecation", "-unchecked", "-Xlint", "-Xdisable-assertions")
ThisBuild / scalaVersion := "2.13.18"

ThisBuild / Test / parallelExecution := false

// Deduplication (assemblyMergeStrategy) for sbt-assembly
val SparkJobAssemblyMergeStrategy: String => sbtassembly.MergeStrategy = {
  // Do not erase log4j files
  case "plugin.properties" | "log4j.properties" =>
    MergeStrategy.concat

  // Otherwise it will fail with "Failed to find the data source: parquet."
  case PathList("META-INF", "services",  _*) =>
    MergeStrategy.concat

  case PathList("META-INF", xs @ _*) =>
    MergeStrategy.discard

  case x =>
    MergeStrategy.first
}


lazy val root = (project in file("."))
  .settings(
    name := "interwalled",
    organization := "me.kosik",
    version := "1.0.0-SNAPSHOT"
  )


val ScalaTestVersion = "3.2.20"
val SparkVersion = "4.1.1"

ThisBuild / libraryDependencies += "org.scalatest"      %% "scalatest"    % ScalaTestVersion
ThisBuild / libraryDependencies += "org.apache.spark"   %% "spark-core"   % SparkVersion      % Provided
ThisBuild / libraryDependencies += "org.apache.spark"   %% "spark-sql"    % SparkVersion      % Provided

// sbt-assembly
ThisBuild / assembly / assemblyJarName := f"interwalled-${version.value}.jar"
ThisBuild / assembly / assemblyMergeStrategy := SparkJobAssemblyMergeStrategy