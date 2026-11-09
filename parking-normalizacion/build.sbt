ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "parking-normalizacion",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.4",
      "org.apache.spark" %% "spark-sql"  % "3.3.4",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.4",
      "org.elasticsearch" %% "elasticsearch-spark-30" % "8.14.3"
    ),

    excludeDependencies ++= Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12")
    ),

    Compile / run / fork := true,
    javaOptions ++= Seq(
      "-Xmx2G",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED"
    )
  )