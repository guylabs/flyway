import org.flywaydb.sbt.FlywayPlugin._

organization := "org.flywaydb"

name := "flyway-sbt-largetest"

libraryDependencies ++= Seq(
  "org.springframework" % "spring-jdbc" % "3.0.5.RELEASE",
  "org.hsqldb" % "hsqldb" % "2.2.8",
  "org.flywaydb" % "flyway-core" % "0-SNAPSHOT"
)

resolvers += (
    "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
)

flywayUser := "SA"

flywayPlaceholders := Map("name" -> "My Name")






