name := "some-streaming-processors"

organization := "com.epam.itw"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.11.8"

val sparkVersion = "2.1.0"

val circeVersion = "0.5.2"

resolvers += Resolver.jcenterRepo
resolvers += Resolver.sonatypeRepo("public")
resolvers += Resolver.mavenLocal
//resolvers += "Artifactory" at "http://ecsa0040015e.epam.com:8081/artifactory/ext-snapshot-local/"

//publishTo := Some("Artifactory Realm" at "http://artifactory.itw:8081/artifactory/ext-snapshot-local/")
publishTo := Some(Resolver.file("file", new File(Path.userHome.absolutePath + "/.m2/repository")))
credentials += Credentials(new File("credentials.properties"))

libraryDependencies ++= Seq(
  "com.iheart" %% "ficus" % "1.2.7",
  "com.typesafe" % "config" % "1.3.1",
  ("org.apache.spark" %% "spark-sql" % sparkVersion % "provided").exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  ("org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion)
    .exclude("org.apache.spark", "spark-tags_2.11")
    .exclude("org.spark-project.spark", "unused")
    .exclude("org.slf4j", "slf4j-api")
    .exclude("org.slf4j", "slf4j-log4j12"),
  ("com.sksamuel.avro4s" %% "avro4s-core" % "1.6.4")
    .exclude("org.slf4j", "slf4j-api")
    .exclude("org.slf4j", "slf4j-log4j12"),
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-jawn" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "com.tdunning" % "t-digest" % "3.2-SNAPSHOT",
  // tests
  "org.apache.spark" %% "spark-streaming" % sparkVersion % Test classifier "tests",
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.6.0" % Test
)

fork in run := true

javaOptions in run ++= Seq("-Xms256M", "-Xmx1G", "-XX:+UseG1GC")

addCompilerPlugin(
  "org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full
)

mainClass in Compile := Some("com.libertyglobal.medm.NetworkLatencyProcessor")

// disable publishing the main API jar
publishArtifact in(Compile, packageDoc) := false

// disable publishing the main sources jar
publishArtifact in(Compile, packageSrc) := false
