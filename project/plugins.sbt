// Version derived from git tags — no version literal in source.
addSbtPlugin("com.github.sbt" % "sbt-dynver" % "5.1.0")

// Packages the service as a runnable app and a Docker image.
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.10.4")

// Formatting (CI runs scalafmtCheckAll).
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.4")

// Static analysis / linting (OrganizeImports + DisableSyntax).
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.13.0")

// Test coverage (CI reports it; add a `coverageMinimumStmtTotal := N` gate in build.sbt once the
// suite is mature — a gate on a fresh scaffold would red the first CI run).
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.12")

// Build-time version info exposed to the app (health endpoint reports version).
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.12.0")

// gRPC codegen for the synchronous read surface (DESIGN §4). The service proto lives in
// this repo as a PROPOSAL (build order step 5: "stub the Lexicon contract locally while the
// proposal is reviewed") — it is not the landed contract, and the Lexicon owns that.
//
// Distinct from the Hermes stubs, which arrive PRE-GENERATED in the lexicon-hermes-grpc jar
// and run no codegen here. This plugin generates only Ariadne's own service.
addSbtPlugin("org.apache.pekko" % "pekko-grpc-sbt-plugin" % "1.1.1")
