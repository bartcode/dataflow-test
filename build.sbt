import sbt._
import Keys._

val scioVersion = "0.8.4"
val beamVersion = "2.19.0"
val scalaMacrosVersion = "2.1.1"

lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
  organization := "example",
  // Semantic versioning http://semver.org/
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.12.10",
  scalacOptions ++= Seq("-target:jvm-1.8",
    "-deprecation",
    "-feature",
    "-unchecked"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
)

lazy val paradiseDependency =
  "org.scalamacros" % "paradise" % scalaMacrosVersion cross CrossVersion.full
lazy val macroSettings = Seq(
  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "com.typesafe" % "config" % "1.4.0",
    "com.twitter" %% "algebird-core" % "0.13.6"),
  addCompilerPlugin(paradiseDependency)
)

PB.targets in Compile := Seq(
  PB.gens.java -> (sourceManaged in Compile).value,
  PB.gens.python -> file("client/src/message"),
  scalapb.gen(javaConversions = true) -> (sourceManaged in Compile).value
)

lazy val root: Project = project
  .in(file("."))
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(
    name := "example",
    description := "example",
    publish / skip := true,
    run / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion,
      "com.spotify" %% "scio-bigquery" % scioVersion,
      "com.spotify" %% "scio-test" % scioVersion % Test,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      "org.slf4j" % "slf4j-simple" % "1.7.25"
    )
  )
  .enablePlugins(PackPlugin)

lazy val repl: Project = project
  .in(file(".repl"))
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(
    name := "repl",
    description := "Scio REPL for example",
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-repl" % scioVersion
    ),
    Compile / mainClass := Some("com.spotify.scio.repl.ScioShell"),
    publish / skip := true,
  )
  .dependsOn(root)
