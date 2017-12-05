name := "Smallstm"

version := "1.0"

val dottyVersion = "0.5.0-RC1"
// val scala212Version = "2.11.11"

lazy val root = (project in file(".")).
  settings(
    name := "dotty-simple",
    version := "0.1.0",
    scalaVersion := dottyVersion,
    libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test"
    //, crossScalaVersions := Seq(dottyVersion, scala212Version)
  )
