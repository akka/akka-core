/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.scaladsl

import scala.util.control.NonFatal

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Outcome
import org.scalatest.TestSuite
import org.slf4j.LoggerFactory

import akka.actor.testkit.typed.internal.CapturingAppender

/**
 * Mixin this trait to a ScalaTest test to make log lines appear only when the test failed.
 *
 * Requires Logback and configuration like the following the logback-test.xml:
 *
 * {{{
 *     <appender name="CapturingAppender" class="akka.actor.testkit.typed.internal.CapturingAppender" />
 *
 *     <logger name="akka.actor.testkit.typed.internal.CapturingAppenderDelegate" >
 *       <appender-ref ref="STDOUT"/>
 *     </logger>
 *
 *     <root level="DEBUG">
 *         <appender-ref ref="CapturingAppender"/>
 *     </root>
 * }}}
 */
trait LogCapturing extends BeforeAndAfterAll { self: TestSuite =>

  // eager access of CapturingAppender to fail fast if misconfigured
  private val capturingAppender = CapturingAppender.get("")

  private val myLogger = LoggerFactory.getLogger(classOf[LogCapturing])

  override protected def afterAll(): Unit = {
    try {
      super.afterAll()
    } catch {
      case NonFatal(e) =>
        myLogger.error("Exception from afterAll", e)
        capturingAppender.flush()
    } finally {
      capturingAppender.clear()
    }
  }

  /**
   * If true, the logs will be cleared after each test.
   *
   * This is not enabled by default for Scala tests, since state may be shared between
   * sequential tests, and logs from earlier tests may be important.
   */
  protected def clearLogsAfterEachTest: Boolean = false

  /**
   * May be called to explicitly clear the captured logs.
   *
   * Useful for when logs are not cleared after each test. If clearing after every test
   * is desirable, overriding [[clearLogsAfterEachTest]] is preferable.
   */
  def clearCapturedLogs(): Unit = capturingAppender.clear()

  abstract override def withFixture(test: NoArgTest): Outcome = {
    myLogger.info(s"${Console.BLUE}Logging started for test [${self.getClass.getName}: ${test.name}]${Console.RESET}")
    val res = test()
    myLogger.info(
      s"${Console.BLUE}Logging finished for test [${self.getClass.getName}: ${test.name}] that [$res]${Console.RESET}")

    if (!(res.isSucceeded || res.isPending)) {
      println(
        s"--> [${Console.BLUE}${self.getClass.getName}: ${test.name}${Console.RESET}] Start of log messages of test that [$res]")
      capturingAppender.flush()
      println(
        s"<-- [${Console.BLUE}${self.getClass.getName}: ${test.name}${Console.RESET}] End of log messages of test that [$res]")
    }

    if (clearLogsAfterEachTest) {
      capturingAppender.clear()
    }

    res
  }
}
