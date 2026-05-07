/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.serialization

import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal

import akka.actor.ExtendedActorSystem
import akka.event.Logging
import akka.remote.ContainerFormats
import akka.serialization.DisabledJavaSerializer
import akka.serialization.SerializationExtension

/**
 * INTERNAL API
 */
private[akka] class ThrowableSupport(system: ExtendedActorSystem) {

  private lazy val serialization = SerializationExtension(system)
  private val payloadSupport = new WrappedPayloadSupport(system)
  private val log = Logging(system, classOf[ThrowableSupport])

  def serializeThrowable(t: Throwable): Array[Byte] = {
    toProtobufThrowable(t).build().toByteArray
  }

  def toProtobufThrowable(t: Throwable): ContainerFormats.Throwable.Builder = {
    val b = ContainerFormats.Throwable.newBuilder().setClassName(t.getClass.getName)
    if (t.getMessage != null)
      b.setMessage(t.getMessage)
    if (t.getCause != null)
      b.setCause(payloadSupport.payloadBuilder(t.getCause))
    val stackTrace = t.getStackTrace
    if (stackTrace != null) {
      var i = 0
      while (i < stackTrace.length) {
        b.addStackTrace(stackTraceElementBuilder(stackTrace(i)))
        i += 1
      }
    }

    b
  }

  private def stackTraceElementBuilder(elem: StackTraceElement): ContainerFormats.StackTraceElement.Builder = {
    val builder = ContainerFormats.StackTraceElement
      .newBuilder()
      .setClassName(elem.getClassName)
      .setMethodName(elem.getMethodName)
      .setLineNumber(elem.getLineNumber)
    val fileName = elem.getFileName
    if (fileName ne null) builder.setFileName(fileName) else builder.setFileName("")
  }

  def deserializeThrowable(bytes: Array[Byte]): Throwable = {
    fromProtobufThrowable(ContainerFormats.Throwable.parseFrom(bytes))
  }

  def fromProtobufThrowable(protoT: ContainerFormats.Throwable): Throwable = {
    val className = protoT.getClassName
    try {
      // Important security note: before creating an instance of from the class name we
      // check that the class is a Throwable and that it has a configured serializer.
      val clazz = system.dynamicAccess.getClassFor[Throwable](className).get
      // this will throw NotSerializableException if no serializer configured, caught below
      val serializer = serialization.serializerFor(clazz)
      if (serializer.isInstanceOf[DisabledJavaSerializer]) {
        val notSerializableException =
          new ThrowableNotSerializableException(protoT.getMessage, "unknown")
        log.debug(
          "Couldn't deserialize [{}] because Java serialization is disabled. Fallback to " +
          "ThrowableNotSerializableException. {}",
          notSerializableException.originalClassName,
          notSerializableException.originalMessage)
        notSerializableException
      } else {

        (if (protoT.hasCause) {
           val cause = payloadSupport.deserializePayload(protoT.getCause).asInstanceOf[Throwable]
           system.dynamicAccess.createInstanceFor[Throwable](
             className,
             List(classOf[String] -> protoT.getMessage, classOf[Throwable] -> cause))
         } else {
           system.dynamicAccess.createInstanceFor[Throwable](clazz, List(classOf[String] -> protoT.getMessage))
         }) match {
          case Success(t) =>
            fillInStackTrace(protoT, t)
            t
          case Failure(e) =>
            new ThrowableNotSerializableException(protoT.getMessage, className, e)
        }
      }
    } catch {
      case NonFatal(e) =>
        new ThrowableNotSerializableException(protoT.getMessage, className, e)
    }
  }

  private def fillInStackTrace(protoT: ContainerFormats.Throwable, t: Throwable): Unit = {
    import scala.jdk.CollectionConverters._
    val stackTrace =
      protoT.getStackTraceList.asScala.map { elem =>
        val fileName = elem.getFileName
        new StackTraceElement(
          elem.getClassName,
          elem.getMethodName,
          if (fileName.length > 0) fileName else null,
          elem.getLineNumber)
      }.toArray
    t.setStackTrace(stackTrace)
  }
}
