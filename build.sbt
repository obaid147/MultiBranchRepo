ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "sensor-task"
  )

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.6.16",
  "com.github.tototoshi" %% "scala-csv" % "1.3.8"

)