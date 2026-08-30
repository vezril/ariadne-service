import com.typesafe.sbt.packager.docker.Cmd

// ---------------------------------------------------------------------------
// ariadne — a Scala 3 + Apache Pekko HTTP service.
//
//   core   — pure domain logic (ZERO Pekko deps), unit-tested.
//   server — Pekko HTTP runtime + Main + Docker image.
//
// Version is derived from git tags by sbt-dynver (project/plugins.sbt); no
// version literal is committed. The dynver separator is Docker-tag-safe ('-').
// ---------------------------------------------------------------------------

ThisBuild / organization := "me.cference.ariadne"
ThisBuild / scalaVersion := "3.3.4" // Scala 3 LTS

ThisBuild / homepage := Some(url("https://github.com/vezril/ariadne-service"))
ThisBuild / licenses := Seq(
  "MIT" -> url("https://github.com/vezril/ariadne-service/blob/main/LICENSE.md")
)
ThisBuild / startYear := Some(2026)
ThisBuild / developers := List(
  Developer(
    id = "vezril",
    name = "Calvin Ference",
    email = "calvin.ference@proton.me",
    url = url("https://github.com/vezril")
  )
)

// sbt-dynver: no version literal committed. Use a Docker-tag-safe separator
// (git describe's default '+' is illegal in image tags).
ThisBuild / dynverSeparator := "-"

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Werror",
  "-Wunused:all"
)

lazy val pekkoVersion = "1.2.0"
lazy val pekkoHttpVersion = "1.2.0"
// Aligned with apollo-storage so pekko-projection (which pulls pekko 1.2.x /
// r2dbc 1.1.x) does not create a mixed-version classpath — Pekko forbids that.
lazy val pekkoR2dbcVersion = "1.1.0"
lazy val pekkoProjectionVersion = "1.1.0"
lazy val testcontainersVersion = "0.41.4"
lazy val scalaTestVersion = "3.2.19"
lazy val scalaCheckPlusVersion = "3.2.19.0"
lazy val logbackVersion = "1.5.16"
lazy val logstashEncoderVersion = "8.0"

// --- root: aggregate only, not published -------------------------------------
lazy val root = (project in file("."))
  .aggregate(core, server)
  .settings(
    name := "ariadne",
    publish / skip := true
  )

// --- core: pure domain logic, no Pekko. --------------------------------------
lazy val core = (project in file("core"))
  .settings(
    name := "ariadne-core",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
      // Property tests for the matching algebra: DESIGN §11 step 3 asks for them by
      // name, because the normaliser and scorer have invariants (idempotence,
      // symmetry, a bounded score) that examples cannot cover exhaustively.
      "org.scalatestplus" %% "scalacheck-1-18" % scalaCheckPlusVersion % Test
    )
  )

// --- server: Pekko runtime + Main + Docker image. ----------------------------
lazy val server = (project in file("server"))
  .dependsOn(core)
  .enablePlugins(JavaAppPackaging, DockerPlugin, BuildInfoPlugin)
  .settings(
    name := "ariadne-server",
    Compile / mainClass := Some("me.cference.ariadne.Main"),
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-stream" % pekkoVersion,
      "org.apache.pekko" %% "pekko-http" % pekkoHttpVersion,
      "org.apache.pekko" %% "pekko-http-spray-json" % pekkoHttpVersion,
      "org.apache.pekko" %% "pekko-slf4j" % pekkoVersion,
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      // Swagger UI served from the CLASSPATH — DESIGN §4 requires self-hosted /docs with
      // zero CDN egress, so the assets ship in the image rather than being fetched.
      "org.webjars" % "swagger-ui" % "5.17.14",
      // Structured JSON logs (the constellation log schema — see the add-structured-logging spec).
      "net.logstash.logback" % "logstash-logback-encoder" % logstashEncoderVersion,
      // --- persistence + read-side projections (DESIGN §2, §3) ---
      // ShardedDaemonProcess distributes the projection instances; on this
      // single-node cluster it still provides the supervised, restart-safe runner.
      "org.apache.pekko" %% "pekko-cluster-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-sharding-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-persistence-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-serialization-jackson" % pekkoVersion,
      "org.apache.pekko" %% "pekko-persistence-r2dbc" % pekkoR2dbcVersion,
      // Explicit since r2dbc 1.1.0 (transitive in 1.0.0).
      "org.postgresql" % "r2dbc-postgresql" % "1.0.7.RELEASE",
      "org.apache.pekko" %% "pekko-projection-r2dbc" % pekkoProjectionVersion,
      "org.apache.pekko" %% "pekko-projection-eventsourced" % pekkoProjectionVersion,
      "org.apache.pekko" %% "pekko-actor-testkit-typed" % pekkoVersion % Test,
      "org.apache.pekko" %% "pekko-persistence-testkit" % pekkoVersion % Test,
      "org.apache.pekko" %% "pekko-projection-testkit" % pekkoProjectionVersion % Test,
      // pekko-projection-testkit 1.1.x pulls pekko-stream-testkit 1.1.3; Pekko forbids a
      // mixed-version classpath, so pin it to pekkoVersion explicitly.
      "org.apache.pekko" %% "pekko-stream-testkit" % pekkoVersion % Test,
      "org.apache.pekko" %% "pekko-http-testkit" % pekkoHttpVersion % Test,
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
      // Real Postgres for projection tests — a mocked journal would prove nothing
      // about the SQL these projections actually run.
      "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersVersion % Test,
      "org.postgresql" % "postgresql" % "42.7.4" % Test
    ),
    // BuildInfo exposes the dynver version to the running app (health endpoint).
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "me.cference.ariadne.build",
    buildInfoOptions += BuildInfoOption.ToJson,
    // --- Docker image (docker.io/calvinference/ariadne) ------------------
    dockerBaseImage := "eclipse-temurin:21-jre",
    dockerExposedPorts := Seq(8080),
    dockerUpdateLatest := false, // release workflow controls :latest explicitly
    Docker / packageName := "ariadne",
    // Image namespace. CI provides DOCKERHUB_USERNAME (single source of truth,
    // matching the workflows); DOCKER_USERNAME is honored for local overrides,
    // then a sensible default so the image builds standalone.
    dockerUsername := Some(
      sys.env
        .get("DOCKERHUB_USERNAME")
        .orElse(sys.env.get("DOCKER_USERNAME"))
        .getOrElse("calvinference")
    ),
    Docker / version := version.value.replace('+', '-'),
    dockerEnvVars := Map("HTTP_PORT" -> "8080", "LOG_FORMAT" -> "json"),
    // Non-root daemon user (process must not run as root).
    Docker / daemonUserUid := Some("1001"),
    Docker / daemonUser := "ariadne",
    // HEALTHCHECK uses bash's /dev/tcp so no extra packages (wget/curl) are
    // needed. bash expands the HTTP_PORT override at runtime.
    dockerCommands ++= Seq(
      Cmd(
        "HEALTHCHECK",
        "--interval=10s --timeout=3s --start-period=20s --retries=5 CMD " +
          """["bash","-c","exec 3<>/dev/tcp/127.0.0.1/${HTTP_PORT:-8080}; """ +
          """printf 'GET /health HTTP/1.0\r\nHost: localhost\r\n\r\n' >&3; """ +
          """grep -q '200 OK' <&3"]"""
      )
    )
  )
