/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package com.typesafe.sbt.multijvm

import java.io.{ BufferedReader, File, InputStreamReader }
import java.lang.{ ProcessBuilder => JProcessBuilder }
import java.util.UUID
import java.util.concurrent.atomic.{ AtomicLong, AtomicReference }

import sbt._
import scala.sys.process.Process
import scala.util.{ Failure, Success, Try }

object Jvm {
  def startJvm(
      javaBin: File,
      jvmOptions: Seq[String],
      runOptions: Seq[String],
      logger: Logger,
      connectInput: Boolean) = {
    forkJava(javaBin, jvmOptions ++ runOptions, logger, connectInput)
  }

  def forkJava(javaBin: File, options: Seq[String], logger: Logger, connectInput: Boolean) = {
    val java = javaBin.toString
    val command = (java :: options.toList).toArray
    val builder = new JProcessBuilder(command: _*)
    Process(builder).run(logger, connectInput)
  }

  /**
   * check if the current operating system is some OS
  **/
  def isOS(os: String) =
    try {
      System.getProperty("os.name").toUpperCase.startsWith(os.toUpperCase)
    } catch {
      case _: Throwable => false
    }

  /**
   * convert to proper path for the operating system
  **/
  def osPath(path: String) = if (isOS("WINDOWS")) Process(Seq("cygpath", path)).lineStream.mkString else path

  def getPodName(hostAndUser: String, sbtLogger: Logger): String = {
    // Retry up to 3 times to handle transient GKE API server connection resets
    @annotation.tailrec
    def attempt(attemptsLeft: Int): String = {
      val command: Array[String] =
        Array("kubectl", "get", "pods", "-l", s"host=$hostAndUser", "--no-headers", "-o", "name")
      val builder = new JProcessBuilder(command: _*)
      sbtLogger.debug("Jvm.getPodName about to run " + command.mkString(" "))
      scala.util.Try(Process(builder).!!) match {
        case scala.util.Success(podName) =>
          sbtLogger.debug("Jvm.getPodName podName is " + podName)
          podName.stripPrefix("pod/").stripSuffix("\n")
        case scala.util.Failure(e) if attemptsLeft <= 1 =>
          throw e
        case scala.util.Failure(e) =>
          sbtLogger.warn(
            s"Jvm.getPodName failed (${e.getMessage}), retrying in 5s (${attemptsLeft - 1} attempt(s) left)")
          Thread.sleep(5000)
          attempt(attemptsLeft - 1)
      }
    }
    attempt(3)
  }

  def syncJar(jarName: String, hostAndUser: String, remoteDir: String, sbtLogger: Logger): Process = {
    // Retry up to 3 times to handle transient GKE API server connection resets
    @annotation.tailrec
    def attempt(attemptsLeft: Int): Int = {
      val podName = getPodName(hostAndUser, sbtLogger)
      val mkdirCommand: Array[String] =
        Array("kubectl", "exec", podName, "--", "/bin/bash", "-c", s"rm -rf $remoteDir && mkdir -p $remoteDir")
      sbtLogger.debug("Jvm.syncJar about to run " + mkdirCommand.mkString(" "))
      val mkdirExitCode = Process(new JProcessBuilder(mkdirCommand: _*)).run(sbtLogger, false).exitValue()
      val exitCode = if (mkdirExitCode == 0) {
        val cpCommand: Array[String] = Array("kubectl", "cp", osPath(jarName), podName + ":" + remoteDir + "/")
        sbtLogger.debug("Jvm.syncJar about to run " + cpCommand.mkString(" "))
        Process(new JProcessBuilder(cpCommand: _*)).run(sbtLogger, false).exitValue()
      } else {
        mkdirExitCode
      }
      if (exitCode == 0 || attemptsLeft <= 1) {
        exitCode
      } else {
        sbtLogger.warn(
          s"Jvm.syncJar failed (exit code $exitCode), retrying in 5s (${attemptsLeft - 1} attempt(s) left)")
        Thread.sleep(5000)
        attempt(attemptsLeft - 1)
      }
    }
    val exitCode = attempt(3)
    new Process {
      def exitValue(): Int = exitCode
      def destroy(): Unit = ()
      def isAlive: Boolean = false
    }
  }

  /**
   * Launch a JVM inside a Kubernetes pod and return a [[Process]] handle for it.
   *
   * Unlike a naive `kubectl exec ...` (which couples the JVM's lifetime to a long-lived
   * SPDY stream that is prone to transient `read: connection reset by peer` failures
   * against the GKE API server), this implementation:
   *
   *   1. uses a single short `kubectl exec` to background-launch the JVM under `setsid`,
   *      with stdout and stderr redirected to separate files in the pod and stdin
   *      redirected to /dev/null, severing the SPDY pipes inherited from kubectl exec.
   *      The launch returns within milliseconds.
   *   2. wraps the JVM in a tiny shell that records its exit code to a sentinel file
   *      when it finishes.
   *   3. returns a [[RemoteProcess]] that drives short, individually retryable
   *      `kubectl exec` polls to stream the JVM's stdout and stderr separately and to
   *      detect completion.
   *
   * The end result is that any single SPDY reset becomes a recoverable hiccup: the JVM
   * keeps running in the pod while the runner is briefly disconnected, and the next
   * poll picks up where the previous one left off.
   *
   * @note `connectInput` is accepted for source compatibility but ignored — the remote
   *       JVM is fully detached from the runner's stdin and cannot read from it.
   */
  def forkRemoteJava(
      java: String,
      jvmOptions: Seq[String],
      appOptions: Seq[String],
      jarName: String,
      hostAndUser: String,
      remoteDir: String,
      logger: Logger,
      connectInput: Boolean,
      sbtLogger: Logger): Process = {
    if (connectInput) {
      sbtLogger.debug("Jvm.forkRemoteJava: ignoring connectInput=true; remote JVM is detached from runner stdin")
    }
    val podName = getPodName(hostAndUser, sbtLogger)
    sbtLogger.debug(s"About to use java $java")
    val shortJarName = new File(jarName).getName
    val javaCommand = List(List(java), jvmOptions, List("-cp", shortJarName), appOptions).flatten
    val javaCmdStr = javaCommand.mkString(" ")

    val runId = UUID.randomUUID().toString
    val outFile = s"/tmp/multi-jvm-$runId.out"
    val errFile = s"/tmp/multi-jvm-$runId.err"
    val exitFile = s"/tmp/multi-jvm-$runId.exit"
    val pidFile = s"/tmp/multi-jvm-$runId.pid"

    // The launch script:
    //   * `rm -f /tmp/multi-jvm-*` cleans up files left behind by previous runs that
    //     crashed before destroy() could clean up. This is safe because each pod hosts
    //     at most one multi-jvm node at a time.
    //   * single-quotes the inner `bash -c` argument so the OUTER shell does not expand
    //     `$?` (which would otherwise be the exit code of `cd`, not the JVM's)
    //   * `setsid` puts the JVM in its own session/process group, so a single
    //     `kill -- -PGID` from destroy() can take down the whole tree
    //   * the redirects on the background command (`</dev/null >OUT 2>ERR`) replace the
    //     SPDY-backed file descriptors inherited from kubectl exec with file FDs in the
    //     pod, so kubectl exec returns immediately and the JVM survives stream resets
    val launchScript =
      s"""rm -f /tmp/multi-jvm-* && cd $remoteDir && {
         |  setsid /bin/bash -c '( $javaCmdStr ; echo $$? > $exitFile )' </dev/null >$outFile 2>$errFile &
         |  echo $$! > $pidFile
         |}""".stripMargin

    val launchCmd = kubectlExec(podName, "/bin/bash", "-c", launchScript)
    sbtLogger.debug(s"Jvm.forkRemoteJava launching: ${launchCmd.mkString(" ")}")
    val launchExit =
      Process(new JProcessBuilder(launchCmd: _*)).run(sbtLogger, false).exitValue()
    if (launchExit != 0) {
      sbtLogger.error(s"Jvm.forkRemoteJava launch failed (exit $launchExit) for pod $podName, runId $runId")
      new Process {
        def exitValue(): Int = launchExit
        def destroy(): Unit = ()
        def isAlive: Boolean = false
      }
    } else {
      new RemoteProcess(podName, runId, outFile, errFile, exitFile, pidFile, logger, sbtLogger)
    }
  }

  // ----- internal: a Process backed by a detached JVM in a Kubernetes pod -----

  /**
   * Build a `kubectl exec` command line for use against [[podName]]. Every kubectl
   * call in the new code paths goes through this helper so that the request timeout
   * is applied uniformly: a stuck SPDY stream causes kubectl to self-terminate
   * within `--request-timeout`, which the poll loop then treats as a recoverable
   * failure and retries.
   */
  private def kubectlExec(podName: String, args: String*): Array[String] =
    Array("kubectl", "--request-timeout=30s", "exec", podName, "--") ++ args

  private object RemoteProcess {

    /** Initial poll interval in milliseconds. */
    val PollIntervalMs: Long = 500L

    /** Maximum back-off when consecutive polls keep failing, in milliseconds. */
    val MaxBackoffMs: Long = 5000L

    /** How long to wait for the stderr-drain helper to finish after the poll process
     *  has exited, in milliseconds. */
    val StderrDrainJoinMs: Long = 1000L

    /** Give up after this many consecutive poll failures (~5 min at the max backoff)
     *  on the assumption that the pod or the API server is unreachable rather than
     *  just blipping. The poll loop then sets `observedExit` to a sentinel and
     *  `exitValue()` reports failure rather than hanging the build forever. */
    val MaxConsecutiveFailures: Int = 60
  }

  /**
   * A [[Process]] whose body is a JVM running detached inside a Kubernetes pod.
   *
   * Each "tick" of the poll loop runs a single short `kubectl exec` that
   *   (a) snapshots the exit-code file (if present),
   *   (b) tails any new stdout lines, prefixed with a per-instance OUT marker,
   *   (c) tails any new stderr lines, prefixed with a per-instance ERR marker,
   *   (d) emits the exit-code sentinel last, so any output appended between (a) and
   *       (b)/(c) is captured before we declare the run finished.
   *
   * The reader uses the markers to route lines to `outputLogger.info` (stdout) or
   * `outputLogger.error` (stderr) — preserving the level distinction that the
   * original streaming `Process.run(logger, ...)` provided.
   *
   * Polls that fail (the most common cause is a transient `connection reset by peer`
   * on the SPDY stream to the API server) are simply retried with bounded back-off,
   * because the JVM in the pod is unaffected. After
   * [[RemoteProcess.MaxConsecutiveFailures]] consecutive failures the poll loop
   * gives up and reports failure to avoid hanging the build forever.
   */
  private final class RemoteProcess(
      podName: String,
      runId: String,
      outFile: String,
      errFile: String,
      exitFile: String,
      pidFile: String,
      outputLogger: Logger,
      sbtLogger: Logger)
      extends Process {

    import RemoteProcess._

    // Per-instance markers — collision-proof against any JVM output because runId is
    // a UUID the test cannot know. The exit sentinel is a prefix; the suffix is the
    // exit code parsed by the reader.
    private val OutMarker: String = s"__MJ_OUT_${runId}__"
    private val ErrMarker: String = s"__MJ_ERR_${runId}__"
    private val ExitSentinelPrefix: String = s"__MJ_EXIT_${runId}__"

    /** 1-based line counters; the next poll asks `tail -n +next` for each stream. */
    private val nextOutLine = new AtomicLong(1L)
    private val nextErrLine = new AtomicLong(1L)

    /** `None` until the wrapper subshell writes the exit-code file. */
    private val observedExit = new AtomicReference[Option[Int]](None)

    @volatile private var stopped: Boolean = false

    private val pollThread: Thread = {
      val t = new Thread(new Runnable {
        def run(): Unit = {
          try pollLoop()
          catch {
            case e: Throwable =>
              sbtLogger.error(
                s"Jvm.RemoteProcess[$runId] poll thread died: ${e.getClass.getSimpleName}: ${e.getMessage}")
              observedExit.compareAndSet(None, Some(-1))
          }
        }
      }, s"multi-jvm-poll-$runId")
      t.setDaemon(true)
      t.start()
      t
    }

    override def exitValue(): Int = {
      try pollThread.join()
      catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
      }
      observedExit.get().getOrElse(-1)
    }

    override def destroy(): Unit = {
      stopped = true
      // Best-effort kill of the JVM's process group inside the pod, then a
      // best-effort cleanup of the temp files. Both calls are short kubectl execs;
      // any failure here is logged at debug level and otherwise ignored — destroy
      // is called on a hot shutdown path and must not throw.
      if (observedExit.get().isEmpty) {
        val killScript =
          s"""if [ -f $pidFile ]; then
             |  PID=$$(cat $pidFile)
             |  kill -- -$$PID 2>/dev/null || kill $$PID 2>/dev/null || true
             |fi""".stripMargin
        runQuietly("/bin/bash", "-c", killScript)
      }
      cleanupTmpFiles()
    }

    /** Best-effort `rm -f` of the per-run files in the pod. Failures are swallowed. */
    private def cleanupTmpFiles(): Unit =
      runQuietly("rm", "-f", outFile, errFile, exitFile, pidFile)

    /** Run a kubectl-exec command against this pod and swallow any failure. */
    private def runQuietly(args: String*): Unit = {
      val cmd = kubectlExec(podName, args: _*)
      Try(Process(new JProcessBuilder(cmd: _*)).run(sbtLogger, false).exitValue())
      ()
    }

    def isAlive: Boolean = observedExit.get().isEmpty && pollThread.isAlive

    private def pollLoop(): Unit = {
      var consecutiveFailures = 0
      while (!stopped && observedExit.get().isEmpty && consecutiveFailures < MaxConsecutiveFailures) {
        if (pollOnce()) consecutiveFailures = 0
        else consecutiveFailures += 1
        if (!stopped && observedExit.get().isEmpty && consecutiveFailures < MaxConsecutiveFailures) {
          val shift = math.min(consecutiveFailures, 3)
          val backoff = math.min(MaxBackoffMs, PollIntervalMs * (1L << shift))
          Thread.sleep(backoff)
        }
      }
      if (consecutiveFailures >= MaxConsecutiveFailures && observedExit.get().isEmpty) {
        sbtLogger.error(
          s"Jvm.RemoteProcess[$runId] giving up after $MaxConsecutiveFailures consecutive poll " +
          "failures; the pod or API server appears to be unreachable")
        observedExit.compareAndSet(None, Some(-1))
      } else if (observedExit.get().exists(_ >= 0)) {
        // Final drain after the JVM has been observed to exit normally: catch any
        // output that was written between the last poll's `tail` and the wrapper
        // subshell writing the exit-code file. Skipped on the give-up path because
        // the polls are failing anyway.
        pollOnce()
      }
      cleanupTmpFiles()
    }

    /**
     * One poll iteration: run a single short `kubectl exec` that prints any new
     * stdout and stderr lines (each preceded by a per-instance marker) and, if the
     * wrapper subshell has finished, the exit-code sentinel. The exit value is
     * captured into a shell variable BEFORE the tails so that any output appended
     * between the snapshot and the tails is still captured by a subsequent poll.
     *
     * Returns true on a clean kubectl exit, false on any failure (which the caller
     * treats as recoverable and retries on the next iteration).
     */
    private def pollOnce(): Boolean = {
      val script =
        s"""E=''
           |if [ -f $exitFile ]; then E=$$(cat $exitFile); fi
           |if [ -f $outFile ]; then echo "$OutMarker"; tail -n +${nextOutLine.get()} $outFile; fi
           |if [ -f $errFile ]; then echo "$ErrMarker"; tail -n +${nextErrLine.get()} $errFile; fi
           |if [ -n "$$E" ]; then echo "${ExitSentinelPrefix}$$E"; fi""".stripMargin
      val cmd = kubectlExec(podName, "/bin/bash", "-c", script)
      val pb = new JProcessBuilder(cmd: _*)

      val attempt = Try {
        val proc = pb.start()
        try {
          // Drain stderr separately so kubectl noise (e.g. "Unable to use a TTY ...")
          // doesn't mix with JVM output. Wrapped in try/catch because closing the
          // stream from the outer finally is the normal exit path.
          val errDrainer = new Thread(new Runnable {
            def run(): Unit = {
              try {
                val r = new BufferedReader(new InputStreamReader(proc.getErrorStream))
                var line = r.readLine()
                while (line != null) {
                  sbtLogger.debug(s"kubectl stderr [$runId]: $line")
                  line = r.readLine()
                }
              } catch {
                case _: Throwable => () // stream closed during shutdown; expected
              }
            }
          }, s"multi-jvm-poll-err-$runId")
          errDrainer.setDaemon(true)
          errDrainer.start()

          val reader = new BufferedReader(new InputStreamReader(proc.getInputStream))
          // The script always emits the OUT section before the ERR section, so OUT
          // is the right default if a line somehow arrives before any marker.
          val consumeOut: String => Unit = { line =>
            outputLogger.info(line)
            nextOutLine.incrementAndGet()
            ()
          }
          val consumeErr: String => Unit = { line =>
            outputLogger.error(line)
            nextErrLine.incrementAndGet()
            ()
          }
          var consume: String => Unit = consumeOut

          var line = reader.readLine()
          while (line != null) {
            if (line == OutMarker) consume = consumeOut
            else if (line == ErrMarker) consume = consumeErr
            else if (line.startsWith(ExitSentinelPrefix)) {
              Try(line.substring(ExitSentinelPrefix.length).toInt).foreach(c =>
                observedExit.compareAndSet(None, Some(c)))
            } else consume(line)
            line = reader.readLine()
          }
          val ec = proc.waitFor()
          errDrainer.join(StderrDrainJoinMs)
          ec
        } finally {
          // Release FDs to avoid leaks across many polls. Closing the input stream
          // first also unblocks the errDrainer if it timed out on the join above.
          Try(proc.getInputStream.close())
          Try(proc.getErrorStream.close())
          Try(proc.getOutputStream.close())
          if (proc.isAlive) Try(proc.destroyForcibly())
          ()
        }
      }

      attempt match {
        case Success(0) => true
        case Success(ec) =>
          sbtLogger.warn(s"Jvm.RemoteProcess[$runId] poll exited $ec (likely transient SPDY reset); will retry")
          false
        case Failure(e) =>
          sbtLogger.warn(
            s"Jvm.RemoteProcess[$runId] poll raised: ${e.getClass.getSimpleName}: ${e.getMessage}; will retry")
          false
      }
    }
  }
}

class JvmBasicLogger(name: String) extends BasicLogger {
  def jvm(message: String) = "[%s] %s".format(name, message)

  def log(level: Level.Value, message: => String) = System.out.synchronized {
    System.out.println(jvm(message))
  }

  def trace(t: => Throwable) = System.out.synchronized {
    val traceLevel = getTrace
    if (traceLevel >= 0) System.out.print(StackTrace.trimmed(t, traceLevel))
  }

  def success(message: => String) = log(Level.Info, message)
  def control(event: ControlEvent.Value, message: => String) = log(Level.Info, message)

  def logAll(events: Seq[LogEvent]) = System.out.synchronized { events.foreach(log) }
}

final class JvmLogger(name: String) extends JvmBasicLogger(name)
