libraryDependencies += Defaults.sbtPluginExtra(
  "com.eed3si9n" % "sbt-assembly" % "2.1.1",
  (pluginCrossBuild / sbtBinaryVersion).value,
  (pluginCrossBuild / scalaBinaryVersion).value)

addSbtPlugin("com.lightbend.sbt" % "sbt-java-formatter" % "0.8.0")
addSbtPlugin("com.lightbend.sbt" % "sbt-bill-of-materials" % "1.0.2")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")
addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.1.3")
addSbtPlugin("com.github.sbt" % "sbt-unidoc" % "0.5.0")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.2")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.5")
addSbtPlugin("io.spray" % "sbt-boilerplate" % "0.6.1")
addSbtPlugin("com.lightbend.akka" % "sbt-paradox-akka" % "0.51")
addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.10.0")
addSbtPlugin("com.hpe.sbt" % "sbt-pull-request-validator" % "1.0.0")
addSbtPlugin("net.bzzt" % "sbt-reproducible-builds" % "0.31")

addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.11.1")
addSbtPlugin("com.lightbend.sbt" % "sbt-publish-rsync" % "0.3")
