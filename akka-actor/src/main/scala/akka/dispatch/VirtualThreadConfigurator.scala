/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch

import akka.actor.DynamicAccess
import akka.annotation.InternalApi
import akka.util.JavaVersion
import com.typesafe.config.Config

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object VirtualThreadConfigurator {

  def virtualThreadsSupported(): Boolean = JavaVersion.majorVersion >= 21

  // Note: since we still support JDK 11 and 17, we need to access the factory method reflectively
  private def createVirtualThreadFactory(
      dynamicAccess: DynamicAccess,
      virtualThreadName: String,
      uncaughtExceptionHandler: Option[Thread.UncaughtExceptionHandler]): ThreadFactory = {
    val ofVirtualMethod =
      try {
        classOf[Thread].getMethod("ofVirtual")
      } catch {
        case _: NoSuchMethodError =>
          throw new IllegalStateException("Virtual thread executors only supported on JDK 21 and newer")
      }
    val ofVirtual = ofVirtualMethod.invoke(null)
    val ofVirtualInterface = dynamicAccess.getClassFor[AnyRef]("java.lang.Thread$Builder$OfVirtual").get

    // thread names
    val ofVirtualWithName =
      if (virtualThreadName.nonEmpty) {
        val nameMethod = ofVirtualInterface.getMethod("name", classOf[String])
        nameMethod.invoke(ofVirtual, virtualThreadName)
      } else ofVirtual

    // uncaught exception handler
    val ofVirtualWithUEH = uncaughtExceptionHandler match {
      case Some(ueh) =>
        val uncaughtExceptionHandlerMethod =
          ofVirtualInterface.getMethod("uncaughtExceptionHandler", classOf[Thread.UncaughtExceptionHandler])
        uncaughtExceptionHandlerMethod.invoke(ofVirtualWithName, ueh)
      case None => ofVirtualWithName
    }
    val factoryMethod = ofVirtualInterface.getMethod("factory")
    factoryMethod.invoke(ofVirtualWithUEH).asInstanceOf[ThreadFactory]
  }

  private def threadPerTaskExecutor(threadFactory: ThreadFactory): ExecutorService = {
    val newThreadPerTaskMethod = classOf[Executors].getMethod("newThreadPerTaskExecutor", classOf[ThreadFactory])
    newThreadPerTaskMethod.invoke(null, threadFactory).asInstanceOf[ExecutorService]
  }

  private class VirtualThreadExecutorServiceFactory(tf: ThreadFactory) extends ExecutorServiceFactory {
    override def createExecutorService: ExecutorService = threadPerTaskExecutor(tf)
  }

}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] class VirtualThreadConfigurator(config: Config, prerequisites: DispatcherPrerequisites)
    extends ExecutorServiceConfigurator(config, prerequisites)
    with NoBatchingExecutorFactoryProvider {
  import VirtualThreadConfigurator._

  override def createExecutorServiceFactory(id: String, threadFactory: ThreadFactory): ExecutorServiceFactory = {

    val tf = threadFactory match {
      case m: MonitorableThreadFactory =>
        // add the dispatcher id to the thread names
        val virtualThreadFactory =
          createVirtualThreadFactory(prerequisites.dynamicAccess, m.name + "-" + id, Some(m.exceptionHandler))

        // Note: daemonic false not allowed for virtual threads, so we ignore that
        m.contextClassLoader.fold[ThreadFactory](virtualThreadFactory)(classLoader =>
          (r: Runnable) => {
            val virtualThread = virtualThreadFactory.newThread(r)
            virtualThread.setContextClassLoader(classLoader)
            virtualThread
          })

      case _ => createVirtualThreadFactory(prerequisites.dynamicAccess, "-" + id, None)
    }
    new VirtualThreadExecutorServiceFactory(tf)
  }

}
