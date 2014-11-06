import sbt._
import Keys._

object ApplicationBuild extends Build {

  val appName         = "ReactiveCouchbase-core"
  val appVersion      = "0.4-SNAPSHOT"
  val appScalaVersion = "2.11.1"
  //val appScalaBinaryVersion = "2.10"
  val appScalaCrossVersions = Seq("2.11.1", "2.10.4")

  val local: Def.Initialize[Option[sbt.Resolver]] = version { (version: String) =>
    val localPublishRepo = "./repository"
    if(version.trim.endsWith("SNAPSHOT"))
      Some(Resolver.file("snapshots", new File(localPublishRepo + "/snapshots")))
    else Some(Resolver.file("releases", new File(localPublishRepo + "/releases")))
  }

  val nexusPublish = version { v =>
    val nexus = "https://oss.sonatype.org/"
    if (v.trim.endsWith("SNAPSHOT"))
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  }

  lazy val baseSettings = Defaults.defaultSettings ++ Seq(
    scalaVersion := appScalaVersion,
    //scalaBinaryVersion := appScalaBinaryVersion,
    crossScalaVersions := appScalaCrossVersions,
    parallelExecution in Test := false
  )

  lazy val root = Project("root", base = file("."))
    .settings(baseSettings: _*)
    .settings(
      publishLocal := {},
      publish := {}
    ).aggregate(
      driver
    )

  lazy val driver = Project(appName, base = file("driver"))
    .settings(baseSettings: _*)
    .settings(
      resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
      libraryDependencies += "com.couchbase.client" % "couchbase-client" % "1.4.5",
      libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.6" cross CrossVersion.binary,
      libraryDependencies += "com.typesafe.play" %% "play-iteratees" % "2.3.5" cross CrossVersion.binary,
      libraryDependencies += "com.typesafe.play" %% "play-json" % "2.3.5" cross CrossVersion.binary,
      libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.1",
      libraryDependencies += "com.ning" % "async-http-client" % "1.8.14",
      libraryDependencies += "com.typesafe" % "config" % "1.2.1",
      libraryDependencies += "org.specs2" %% "specs2" % "2.3.12" % "test" cross CrossVersion.binary,
      libraryDependencies += "com.codahale.metrics" % "metrics-core" % "3.0.1",
      organization := "org.reactivecouchbase",
      version := appVersion,
      publishTo <<= local,
      publishMavenStyle := true,
      publishArtifact in Test := false,
      pomIncludeRepository := { _ => false },
      pomExtra := (
        <url>http://reactivecouchbase.org</url>
          <licenses>
            <license>
              <name>Apache 2</name>
              <url>http://www.apache.org/licenses/LICENSE-2.0</url>
              <distribution>repo</distribution>
            </license>
          </licenses>
          <scm>
            <url>git@github.com:ReactiveCouchbase/ReactiveCouchbase-core.git</url>
            <connection>scm:git:git@github.com:ReactiveCouchbase/ReactiveCouchbase-core.git</connection>
          </scm>
          <developers>
            <developer>
              <id>mathieu.ancelin</id>
              <name>Mathieu ANCELIN</name>
              <url>https://github.com/mathieuancelin</url>
            </developer>
          </developers>)
    )
}
