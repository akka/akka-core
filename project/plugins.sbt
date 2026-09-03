libraryDependencies += Defaults.sbtPluginExtra(
  "com.eed3si9n" % "sbt-assembly" % "2.4.1",
  (pluginCrossBuild / sbtBinaryVersion).value,
  (pluginCrossBuild / scalaBinaryVersion).value)

addSbtPlugin("com.github.sbt" % "sbt-java-formatter" % "0.12.0")
addSbtPlugin("com.lightbend.sbt" % "sbt-bill-of-materials" % "1.0.2")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")
addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.1.6")
addSbtPlugin("com.github.sbt" % "sbt-unidoc" % "0.6.1")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.3")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.8")
addSbtPlugin("com.github.sbt" % "sbt-boilerplate" % "0.8.1")
addSbtPlugin("io.akka" % "sbt-paradox-akka" % "25.10.2")

addSbtPlugin("com.github.sbt" % "sbt-header" % "5.11.0")
addSbtPlugin("com.github.sbt" % "sbt-pull-request-validator" % "2.0.0")
addSbtPlugin("net.bzzt" % "sbt-reproducible-builds" % "0.35")

addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.11.2")
addSbtPlugin("com.lightbend.sbt" % "sbt-publish-rsync" % "0.3")
// create pom.xml for scanning tools
addSbtPlugin("io.akka.sbt" % "sbt-artifact-bom" % "0.2.0")
