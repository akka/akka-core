/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka

import akka.JdkOptions.autoImport._
import com.lightbend.paradox.projectinfo.ParadoxProjectInfoPluginKeys._
import com.typesafe.sbt.MultiJvmPlugin.autoImport.MultiJvm
import sbt.Def
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyPlugin.autoImport._

import java.io.FileInputStream
import java.io.InputStreamReader
import java.util.Properties

object AkkaBuild {

  object CliOptions {
    // CI is the env var defined by Github Actions and Travis:
    // - https://docs.github.com/en/actions/reference/environment-variables#default-environment-variables
    // - https://docs.travis-ci.com/user/environment-variables/#default-environment-variables
    val runningOnCi: CliOption[Boolean] = CliOption("akka.ci-server", sys.env.contains("CI"))
  }

  val enableMiMa = true

  val parallelExecutionByDefault = false // TODO: enable this once we're sure it does not break things

  lazy val buildSettings = Def.settings(organization := "com.typesafe.akka")

  lazy val rootSettings = Def.settings(
    commands += switchVersion,
    UnidocRoot.akkaSettings,
    Protobuf.settings,
    GlobalScope / parallelExecution := System
        .getProperty("akka.parallelExecution", parallelExecutionByDefault.toString)
        .toBoolean,
    // used for linking to API docs (overwrites `project-info.version`)
    ThisBuild / projectInfoVersion := { if (isSnapshot.value) "snapshot" else version.value })

  lazy val mayChangeSettings = Seq(description := """|This module of Akka is marked as
                      |'may change', which means that it is in early
                      |access mode, which also means that it is not covered
                      |by commercial support. An module marked 'may change' doesn't
                      |have to obey the rule of staying binary compatible
                      |between minor releases. Breaking API changes may be
                      |introduced in minor releases without notice as we
                      |refine and simplify based on your feedback. Additionally
                      |such a module may be dropped in major releases
                      |without prior deprecation.
                      |""".stripMargin)

  val (mavenLocalResolver, mavenLocalResolverSettings) =
    System.getProperty("akka.build.M2Dir") match {
      case null => (Resolver.mavenLocal, Seq.empty)
      case path =>
        // Maven resolver settings
        def deliverPattern(outputPath: File): String =
          (outputPath / "[artifact]-[revision](-[classifier]).[ext]").absolutePath

        val resolver = Resolver.file("user-publish-m2-local", new File(path))
        (
          resolver,
          Seq(
            otherResolvers := resolver :: publishTo.value.toList,
            publishM2Configuration := Classpaths.publishConfig(
                publishMavenStyle.value,
                deliverPattern(crossTarget.value),
                if (isSnapshot.value) "integration" else "release",
                ivyConfigurations.value.map(c => ConfigRef(c.name)).toVector,
                artifacts = packagedArtifacts.value.toVector,
                resolverName = resolver.name,
                checksums = (publishM2 / checksums).value.toVector,
                logging = ivyLoggingLevel.value,
                overwrite = true)))
    }

  lazy val resolverSettings = Def.settings(
    // should we be allowed to use artifacts published to the local maven repository
    if (System.getProperty("akka.build.useLocalMavenResolver", "false").toBoolean)
      resolvers += mavenLocalResolver
    else Seq.empty,
    // should we be allowed to use artifacts from sonatype snapshots
    if (System.getProperty("akka.build.useSnapshotSonatypeResolver", "false").toBoolean)
      resolvers ++= Resolver.sonatypeOssRepos("snapshots")
    else Seq.empty,
    pomIncludeRepository := (_ => false) // do not leak internal repositories during staging
  )

  private def allWarnings: Boolean = System.getProperty("akka.allwarnings", "false").toBoolean

  final val DefaultScalacOptions = Def.setting {
    if (scalaVersion.value.startsWith("3.")) {
      Seq(
        "-encoding",
        "UTF-8",
        "-feature",
        "-unchecked",
        // 'blessed' since 2.13.1
        "-language:higherKinds")
    } else {
      Seq(
        "-encoding",
        "UTF-8",
        "-feature",
        "-unchecked",
        "-Xlog-reflective-calls",
        // 'blessed' since 2.13.1
        "-language:higherKinds")
    }
  }

  private def jvmGCLogOptions(isJdk11OrHigher: Boolean, isJdk8: Boolean): Seq[String] = {
    if (isJdk11OrHigher)
      // -Xlog:gc* is equivalent to -XX:+PrintGCDetails. See:
      // https://docs.oracle.com/en/java/javase/11/tools/java.html#GUID-BE93ABDC-999C-4CB5-A88B-1994AAAC74D5
      Seq("-Xlog:gc*")
    else if (isJdk8) Seq("-XX:+PrintGCTimeStamps", "-XX:+PrintGCDetails")
    else Nil
  }

  // -XDignore.symbol.file suppresses sun.misc.Unsafe warnings
  final val DefaultJavacOptions = Seq("-encoding", "UTF-8", "-Xlint:unchecked", "-XDignore.symbol.file")

  lazy val defaultSettings: Seq[Setting[_]] = Def.settings(
    Dependencies.Versions,
    resolverSettings,
    TestExtras.Filter.settings,
    // compile options
    Compile / scalacOptions ++= DefaultScalacOptions.value,
    Compile / scalacOptions ++=
      JdkOptions.targetJdkScalacOptions(
        targetSystemJdk.value,
        optionalDir(jdk8home.value),
        fullJavaHomes.value,
        scalaVersion.value),
    Compile / scalacOptions ++= (if (allWarnings) Seq("-deprecation") else Nil),
    Test / scalacOptions := (Test / scalacOptions).value.filterNot(opt =>
        opt == "-Xlog-reflective-calls" || opt.contains("genjavadoc")),
    Compile / javacOptions ++= {
      DefaultJavacOptions ++
      JdkOptions.targetJdkJavacOptions(targetSystemJdk.value, optionalDir(jdk8home.value), fullJavaHomes.value)
    },
    Test / javacOptions ++= DefaultJavacOptions ++
      JdkOptions.targetJdkJavacOptions(targetSystemJdk.value, optionalDir(jdk8home.value), fullJavaHomes.value),
    Compile / javacOptions ++= (if (allWarnings) Seq("-Xlint:deprecation") else Nil),
    doc / javacOptions := Seq(),
    crossVersion := CrossVersion.binary,
    // Adds a `src/main/scala-2.13+` source directory for code shared
    // between Scala 2.13 and Scala 3
    Compile / unmanagedSourceDirectories ++= {
      val sourceDir = (Compile / sourceDirectory).value
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3, n))            => Seq(sourceDir / "scala-2.13+")
        case Some((2, n)) if n >= 13 => Seq(sourceDir / "scala-2.13+")
        case _                       => Nil
      }
    },
    ThisBuild / ivyLoggingLevel := UpdateLogging.Quiet,
    licenses := {
      val tagOrBranch =
        if (isSnapshot.value) "main"
        else "v" + version.value
      Seq(("BUSL-1.1", url(s"https://raw.githubusercontent.com/akka/akka/${tagOrBranch}/LICENSE")))
    },
    homepage := Some(url("https://akka.io/")),
    description := "Akka is a toolkit for building highly concurrent, distributed, and resilient message-driven applications for Java and Scala.",
    scmInfo := Some(
        ScmInfo(
          url("https://github.com/akka/akka"),
          "scm:git:https://github.com/akka/akka.git",
          "scm:git:git@github.com:akka/akka.git")),
    releaseNotesURL := (
        if (isSnapshot.value) None
        else Some(url(s"https://github.com/akka/akka/releases/tag/v${version.value}"))
      ),
    apiURL := Some(url(s"https://doc.akka.io/api/akka/${version.value}")),
    initialCommands :=
      """|import language.postfixOps
         |import akka.actor._
         |import scala.concurrent._
         |import com.typesafe.config.ConfigFactory
         |import scala.concurrent.duration._
         |import akka.util.Timeout
         |var config = ConfigFactory.parseString("akka.stdout-loglevel=INFO,akka.loglevel=DEBUG,pinned{type=PinnedDispatcher,executor=thread-pool-executor,throughput=1000}")
         |var remoteConfig = ConfigFactory.parseString("akka.remote.artery.canonical.port=0,akka.actor.provider=remote").withFallback(config)
         |var system: ActorSystem = null
         |implicit def _system: ActorSystem = system
         |def startSystem(remoting: Boolean = false) = { system = ActorSystem("repl", if(remoting) remoteConfig else config); println("don’t forget to system.terminate()!") }
         |implicit def ec: ExecutionContext = system.dispatcher
         |implicit val timeout: Timeout = Timeout(5 seconds)
         |""".stripMargin,
    /**
     * Test settings
     */
    Test / fork := true,
    // default JVM config for tests
    Test / javaOptions ++= {
      val defaults = Seq(
        // ## core memory settings
        "-XX:+UseG1GC",
        // most tests actually don't really use _that_ much memory (>1g usually)
        // twice used (and then some) keeps G1GC happy - very few or to no full gcs
        "-Xms3g",
        "-Xmx3g",
        // increase stack size (todo why?)
        "-Xss2m",
        // ## extra memory/gc tuning
        // this breaks jstat, but could avoid costly syncs to disc see https://www.evanjones.ca/jvm-mmap-pause.html
        "-XX:+PerfDisableSharedMem",
        // tell G1GC that we would be really happy if all GC pauses could be kept below this as higher would
        // likely start causing test failures in timing tests
        "-XX:MaxGCPauseMillis=300",
        // nio direct memory limit for artery/aeron (probably)
        "-XX:MaxDirectMemorySize=256m",
        // faster random source
        "-Djava.security.egd=file:/dev/./urandom")

      defaults ++ CliOptions.runningOnCi
        .ifTrue(jvmGCLogOptions(JdkOptions.isJdk11orHigher, JdkOptions.isJdk8))
        .getOrElse(Nil) ++
      JdkOptions.versionSpecificJavaOptions
    },
    // all system properties passed to sbt prefixed with "akka." or "aeron." will be passed on to the forked jvms as is
    Test / javaOptions := {
      val base = (Test / javaOptions).value
      val knownPrefix = Set("akka.", "aeron.")
      val akkaSysProps: Seq[String] =
        sys.props.iterator.collect {
          case (key, value) if knownPrefix.exists(pre => key.startsWith(pre)) => s"-D$key=$value"
        }.toList

      base ++ akkaSysProps
    },
    // with forked tests the working directory is set to each module's home directory
    // rather than the Akka root, some tests depend on Akka root being working dir, so reset
    Test / testGrouping := {
      val original: Seq[Tests.Group] = (Test / testGrouping).value

      original.map { group =>
        group.runPolicy match {
          case Tests.SubProcess(forkOptions) =>
            // format: off
            group.withRunPolicy(Tests.SubProcess(
              forkOptions.withWorkingDirectory(workingDirectory = Some(new File(System.getProperty("user.dir"))))))
            // format: on
          case _ => group
        }
      }
    },
    Test / parallelExecution := System
        .getProperty("akka.parallelExecution", parallelExecutionByDefault.toString)
        .toBoolean,
    Test / logBuffered := System.getProperty("akka.logBufferedTests", "false").toBoolean,
    // show full stack traces and test case durations
    Test / testOptions += Tests.Argument("-oDF"),
    mavenLocalResolverSettings,
    docLintingSettings,
    JdkOptions.targetJdkSettings,
    // a workaround for https://github.com/akka/akka/issues/27661
    // see also project/Protobuf.scala that introduces /../ to make "intellij happy"
    MultiJvm / assembly / fullClasspath := {
      val old = (MultiJvm / assembly / fullClasspath).value.toVector
      val files = old.map(_.data.getCanonicalFile).distinct
      files.map { x =>
        Attributed.blank(x)
      }
    })

  lazy val welcomeSettings: Seq[Setting[_]] =
    CliOptions.runningOnCi
      .ifTrue(Seq())
      .getOrElse(Seq(onLoadMessage :=
        s"""
           |
           |                        .~!77!^                            :::          :::
           |                     .^!???????~                          .5GP:        .5GP:
           |                   :~???????????!              ...        .PGP:      . .5GP:      .     ...
           |                .~7??????????????!          ~J5PPP5J!Y5Y. .5GP:  .?557..5GP:  .?557..!J5PPP5?!55J
           |             .^!??????????????????7.      :5GGY!~~!JPGGP. .5GP:.7PGY^  .5GP:.7PGY^ ^5GGJ!~~!YGGG5
           |           :~7?????????????????????7.     YGG!      ^PGP. .5GP?PG5^    .5GP?PG5^  .PGP^      ~GGY
           |        .^7?????????????JJYY55555YYJ?.   .5GG^      .PGP. .5GGG5GP7.   .5GGG5GP7. :PGP:      :PGY
           |      :!????????????JY5PPGGGGGGGGGGGP5^   ~PGP7:..:!5GGP: .PGG~ ~5BP!  .5GG~ ~5BP! !GG5!:..:7PGG5.
           |   .~7??????????JY5PGGGGGGGGGGGGGGGGGGP^   :?5GGPPPPJJPGG7 5GP:  .7PGY^.5GP:   7PGY^^JPGGPPPP?JPGP!
           |  :??????????JYPGGGGPPPPPGGGGGGGGGGGGGGJ     .:^^^:.  :^^: :::     .::: :::     .:::  .:^^^:.  :^^.
           |  ^????????7???7~~^::::::^^~!?Y5GGGGGGG7
           |   ^!7??7~:.                   .^7YPP57
           |     ...                           .:.
           |                                                                                                      
           | ${version.value}
           |
           |
           | Useful sbt tasks:
           | > testOnly *.AnySpec         Only run a selected test
           | > verifyCodeStyle            Verify code style
           | > applyCodeStyle             Apply code style
           | > sortImports                Sort the imports
           | > mimaReportBinaryIssues     Check binary issues
           | > validatePullRequest        Validate pull request
           | > akka-docs/paradox          Build documentation
           | > akka-docs/paradoxBrowse    Browse the generated documentation
           |
           | Contributing guide: https://github.com/akka/akka/blob/main/CONTRIBUTING.md
           | tips: prefix commands with + to run against cross Scala versions.
           |
           |""".stripMargin))

  private def optionalDir(path: String): Option[File] =
    Option(path).filter(_.nonEmpty).map { path =>
      val dir = new File(path)
      if (!dir.exists)
        throw new IllegalArgumentException(s"Path [$path] not found")
      dir
    }

  lazy val docLintingSettings = Seq(
    compile / javacOptions ++= Seq("-Xdoclint:none"),
    test / javacOptions ++= Seq("-Xdoclint:none"),
    doc / javacOptions ++= {
      if (JdkOptions.isJdk8) Seq("-Xdoclint:none")
      else Seq("-Xdoclint:none", "--ignore-source-errors")
    })

  def loadSystemProperties(fileName: String): Unit = {
    import scala.collection.JavaConverters._
    val file = new File(fileName)
    if (file.exists()) {
      println("Loading system properties from file `" + fileName + "`")
      val in = new InputStreamReader(new FileInputStream(file), "UTF-8")
      val props = new Properties
      props.load(in)
      in.close()
      sys.props ++ props.asScala
    }
  }

  def majorMinor(version: String): Option[String] = """\d+\.\d+""".r.findFirstIn(version)

  // So we can `sbt "+~ 3 clean compile"`
  //
  // The advantage over `++` is twofold:
  // * `++` also requires the patch version, `+~` finds the first supported Scala version that matches the prefix (if any)
  // * When subprojects need to be excluded, ++ needs to be specified for each command
  //
  // So the `++` equivalent of the above example is `sbt "++ 3.1.2 clean" "++ 3.1.2 compile"`
  val switchVersion: Command = Command.args("+~", "<version> <args>")({ (initialState: State, args: Seq[String]) =>
    {
      val requestedVersionPrefix = args.head
      val requestedVersion = Dependencies.allScalaVersions.filter(_.startsWith(requestedVersionPrefix)).head

      def run(state: State, command: String): State = {
        val parsed = s"++ $requestedVersion $command".foldLeft(Cross.switchVersion.parser(state))((p, i) => p.derive(i))
        parsed.resultEmpty match {
          case e: sbt.internal.util.complete.Parser.Failure =>
            throw new IllegalStateException(e.errors.mkString(", "))
          case sbt.internal.util.complete.Parser.Value(v) =>
            v()
        }
      }
      val commands = args.tail
      commands.foldLeft(initialState)(run)
    }
  })
}
