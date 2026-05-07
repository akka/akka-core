/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.event.slf4j

import scala.annotation.nowarn

import org.slf4j.{ MDC, Marker, MarkerFactory, Logger => SLFLogger, LoggerFactory => SLFLoggerFactory }

import akka.actor._
import akka.dispatch.RequiresMessageQueue
import akka.event.{ LogMarker, _ }
import akka.event.Logging._
import akka.util.Helpers

/**
 * Base trait for all classes that wants to be able use the SLF4J logging infrastructure.
 */
trait SLF4JLogging {
  @transient
  lazy val log = Logger(this.getClass.getName)
}

/**
 * Logger is a factory for obtaining SLF4J-Loggers
 */
object Logger {

  /**
   * @param logger - which logger
   * @return a Logger that corresponds for the given logger name
   */
  def apply(logger: String): SLFLogger = SLFLoggerFactory.getLogger(logger)

  /**
   * @param logClass - the class to log for
   * @param logSource - the textual representation of the source of this log stream
   * @return a Logger for the specified parameters
   */
  def apply(logClass: Class[_], logSource: String): SLFLogger = logClass match {
    case c if c == classOf[DummyClassForStringSources] => apply(logSource)
    case _                                             => SLFLoggerFactory.getLogger(logClass)
  }

  /**
   * Returns the SLF4J Root Logger
   */
  def root: SLFLogger = apply(SLFLogger.ROOT_LOGGER_NAME)
}

/**
 * SLF4J logger.
 *
 * The thread in which the logging was performed is captured in
 * Mapped Diagnostic Context (MDC) with attribute name "sourceThread".
 */
class Slf4jLogger extends Actor with SLF4JLogging with RequiresMessageQueue[LoggerMessageQueueSemantics] {

  val mdcThreadAttributeName = "sourceThread"
  val mdcActorSystemAttributeName = "sourceActorSystem"
  val mdcAkkaSourceAttributeName = "akkaSource"
  val mdcAkkaTimestamp = "akkaTimestamp"
  val mdcAkkaAddressAttributeName = "akkaAddress"
  val mdcAkkaUidAttributeName = "akkaUid"

  private def akkaAddress = context.system.asInstanceOf[ExtendedActorSystem].provider.addressString
  private def akkaUid: String = {
    try {
      context.system.asInstanceOf[ExtendedActorSystem].uid.toString
    } catch {
      case _: IllegalStateException =>
        // when logging before generating the uid (transport binding when remoting is enabled)
        ""
    }
  }

  def receive = {

    case event @ Error(cause, logSource, logClass, message) =>
      withMdc(logSource, event) {
        cause match {
          case Error.NoCause | null =>
            Logger(logClass, logSource).error(markerIfPresent(event), if (message != null) message.toString else null)
          case _ =>
            Logger(logClass, logSource).error(
              markerIfPresent(event),
              if (message != null) message.toString else cause.getLocalizedMessage,
              cause)
        }
      }

    case event @ Warning(logSource, logClass, message) =>
      withMdc(logSource, event) {
        event match {
          case e: LogEventWithCause =>
            Logger(logClass, logSource).warn(
              markerIfPresent(event),
              if (message != null) message.toString else e.cause.getLocalizedMessage,
              e.cause)
          case _ =>
            Logger(logClass, logSource).warn(markerIfPresent(event), if (message != null) message.toString else null)
        }
      }

    case event @ Info(logSource, logClass, message) =>
      withMdc(logSource, event) {
        Logger(logClass, logSource).info(markerIfPresent(event), "{}", message: Any)
      }

    case event @ Debug(logSource, logClass, message) =>
      withMdc(logSource, event) {
        Logger(logClass, logSource).debug(markerIfPresent(event), "{}", message: Any)
      }

    case InitializeLogger(_) =>
      log.info("Slf4jLogger started")
      sender() ! LoggerInitialized
  }

  @inline
  final def withMdc(logSource: String, logEvent: LogEvent)(logStatement: => Unit): Unit = {
    logEvent match {
      case m: LogEventWithMarker if m.marker ne null =>
        val properties = m.marker.properties
        if (properties.nonEmpty) {
          properties.foreach { case (k, v) => MDC.put(k, String.valueOf(v)) }
        }
      case _ =>
    }

    MDC.put(mdcAkkaSourceAttributeName, logSource)
    MDC.put(mdcThreadAttributeName, logEvent.thread.getName)
    MDC.put(mdcAkkaTimestamp, formatTimestamp(logEvent.timestamp))
    MDC.put(mdcActorSystemAttributeName, context.system.name)
    MDC.put(mdcAkkaAddressAttributeName, akkaAddress)
    val uid = akkaUid
    if (uid != "")
      MDC.put(mdcAkkaUidAttributeName, uid)
    logEvent.mdc.foreach { case (k, v) => MDC.put(k, String.valueOf(v)) }

    try logStatement
    finally {
      MDC.clear()
    }
  }

  private final def markerIfPresent(event: LogEvent): Marker =
    event match {
      case m: LogEventWithMarker =>
        m.marker match {
          case null                        => null
          case slf4jMarker: Slf4jLogMarker => slf4jMarker.marker
          case marker                      => MarkerFactory.getMarker(marker.name)
        }
      case _ => null
    }

  /**
   * Override this method to provide a differently formatted timestamp
   * @param timestamp a "currentTimeMillis"-obtained timestamp
   * @return the given timestamp as a UTC String
   */
  protected def formatTimestamp(timestamp: Long): String =
    Helpers.currentTimeMillisToUTCString(timestamp)

}

/**
 * [[akka.event.LoggingFilter]] that uses the log level defined in the SLF4J
 * backend configuration (e.g. logback.xml) to filter log events before publishing
 * the log events to the `eventStream`.
 */
class Slf4jLoggingFilter(@nowarn("msg=never used") settings: ActorSystem.Settings, eventStream: EventStream)
    extends LoggingFilterWithMarker {
  def isErrorEnabled(logClass: Class[_], logSource: String) =
    (eventStream.logLevel >= ErrorLevel) && Logger(logClass, logSource).isErrorEnabled
  def isWarningEnabled(logClass: Class[_], logSource: String) =
    (eventStream.logLevel >= WarningLevel) && Logger(logClass, logSource).isWarnEnabled
  def isInfoEnabled(logClass: Class[_], logSource: String) =
    (eventStream.logLevel >= InfoLevel) && Logger(logClass, logSource).isInfoEnabled
  def isDebugEnabled(logClass: Class[_], logSource: String) =
    (eventStream.logLevel >= DebugLevel) && Logger(logClass, logSource).isDebugEnabled

  private def slf4jMarker(marker: LogMarker) = marker match {
    case null                        => null
    case slf4jMarker: Slf4jLogMarker => slf4jMarker.marker
    case marker                      => MarkerFactory.getMarker(marker.name)
  }

  override def isErrorEnabled(logClass: Class[_], logSource: String, marker: LogMarker): Boolean =
    (eventStream.logLevel >= ErrorLevel) && Logger(logClass, logSource).isErrorEnabled(slf4jMarker(marker))
  override def isWarningEnabled(logClass: Class[_], logSource: String, marker: LogMarker): Boolean =
    (eventStream.logLevel >= WarningLevel) && Logger(logClass, logSource).isWarnEnabled(slf4jMarker(marker))
  override def isInfoEnabled(logClass: Class[_], logSource: String, marker: LogMarker): Boolean =
    (eventStream.logLevel >= InfoLevel) && Logger(logClass, logSource).isInfoEnabled(slf4jMarker(marker))
  override def isDebugEnabled(logClass: Class[_], logSource: String, marker: LogMarker): Boolean =
    (eventStream.logLevel >= DebugLevel) && Logger(logClass, logSource).isDebugEnabled(slf4jMarker(marker))

}

/** Wraps [[org.slf4j.Marker]] */
final class Slf4jLogMarker(val marker: org.slf4j.Marker) extends LogMarker(name = marker.getName, Map.empty)

/** Factory for creating [[LogMarker]] that wraps [[org.slf4j.Marker]] */
object Slf4jLogMarker {
  def apply(marker: org.slf4j.Marker): Slf4jLogMarker = new Slf4jLogMarker(marker)

  /** Java API */
  def create(marker: org.slf4j.Marker): Slf4jLogMarker = apply(marker)
}
