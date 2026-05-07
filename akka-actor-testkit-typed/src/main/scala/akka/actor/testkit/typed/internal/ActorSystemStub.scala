/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.internal

import java.time.LocalDate
import java.util.Optional
import java.util.concurrent.{ CompletionStage, ThreadFactory }

import scala.annotation.nowarn
import scala.jdk.FutureConverters.FutureOps
import scala.concurrent._

import com.typesafe.config.{ Config, ConfigFactory }
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import akka.{ actor => classic }
import akka.Done
import akka.actor.{ ActorPath, ActorRefProvider, Address, ReflectiveDynamicAccess }
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.DispatcherSelector
import akka.actor.typed.Dispatchers
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.Props
import akka.actor.typed.Scheduler
import akka.actor.typed.Settings
import akka.actor.typed.internal.ActorRefImpl
import akka.actor.typed.internal.InternalRecipientRef
import akka.actor.typed.receptionist.Receptionist
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@nowarn
@InternalApi private[akka] final class ActorSystemStub(
    val name: String,
    config: Config = ActorSystemStub.config.defaultReference)
    extends ActorSystem[Nothing]
    with ActorRef[Nothing]
    with ActorRefImpl[Nothing]
    with InternalRecipientRef[Nothing] {

  private val rootPath: ActorPath = classic.RootActorPath(classic.Address("akka", name))

  override val path: classic.ActorPath = rootPath / "user"

  override val settings: Settings = {
    val classLoader = getClass.getClassLoader
    val dynamicAccess = new ReflectiveDynamicAccess(classLoader)
    val config_ =
      classic.ActorSystem.Settings.amendSlf4jConfig(config, dynamicAccess)
    val untypedSettings = new classic.ActorSystem.Settings(classLoader, config_, name)
    new Settings(untypedSettings)
  }

  override def tell(message: Nothing): Unit =
    throw new UnsupportedOperationException("must not send message to ActorSystemStub")

  // impl ActorRefImpl
  override def isLocal: Boolean = true
  // impl ActorRefImpl
  override def sendSystem(signal: akka.actor.typed.internal.SystemMessage): Unit =
    throw new UnsupportedOperationException("must not send SYSTEM message to ActorSystemStub")

  // impl InternalRecipientRef, ask not supported
  override def provider: ActorRefProvider = throw new UnsupportedOperationException("no provider")

  // stream materialization etc. using stub not supported
  override def classicSystem =
    throw new UnsupportedOperationException("no classic actor system available")

  // impl InternalRecipientRef
  def isTerminated: Boolean = whenTerminated.isCompleted

  val deadLettersInbox = new DebugRef[Any](path.parent / "deadLetters", true)
  override def deadLetters[U]: ActorRef[U] = deadLettersInbox

  override def ignoreRef[U]: ActorRef[U] = deadLettersInbox

  val receptionistInbox = new TestInboxImpl[Receptionist.Command](path.parent / "receptionist")

  override def receptionist: ActorRef[Receptionist.Command] = receptionistInbox.ref

  val controlledExecutor = new ControlledExecutor
  implicit override def executionContext: scala.concurrent.ExecutionContextExecutor = controlledExecutor
  override def dispatchers: akka.actor.typed.Dispatchers = new Dispatchers {
    def lookup(selector: DispatcherSelector): ExecutionContextExecutor = controlledExecutor
    def shutdown(): Unit = ()
  }

  override def dynamicAccess: classic.DynamicAccess = new classic.ReflectiveDynamicAccess(getClass.getClassLoader)

  override def logConfiguration(): Unit = log.info(settings.toString)

  override def scheduler: Scheduler = throw new UnsupportedOperationException("no scheduler")

  private val terminationPromise = Promise[Done]()
  override def terminate(): Unit = terminationPromise.trySuccess(Done)
  override def whenTerminated: Future[Done] = terminationPromise.future
  override def getWhenTerminated: CompletionStage[Done] = whenTerminated.asJava
  override val startTime: Long = System.currentTimeMillis()
  override def uptime: Long = System.currentTimeMillis() - startTime
  override def threadFactory: java.util.concurrent.ThreadFactory = new ThreadFactory {
    override def newThread(r: Runnable): Thread = new Thread(r)
  }

  override def printTree: String = "no tree for ActorSystemStub"

  override def systemActorOf[U](behavior: Behavior[U], name: String, props: Props): ActorRef[U] = {
    throw new UnsupportedOperationException("ActorSystemStub cannot create system actors")
  }

  override def registerExtension[T <: Extension](ext: ExtensionId[T]): T =
    throw new UnsupportedOperationException("ActorSystemStub cannot register extensions")

  override def extension[T <: Extension](ext: ExtensionId[T]): T =
    throw new UnsupportedOperationException("ActorSystemStub cannot register extensions")

  override def hasExtension(ext: ExtensionId[_ <: Extension]): Boolean =
    throw new UnsupportedOperationException("ActorSystemStub cannot register extensions")

  override def log: Logger = LoggerFactory.getLogger(getClass)

  def address: Address = rootPath.address

  override def licenseKeyExpiry: Option[LocalDate] =
    throw new UnsupportedOperationException("licenseKeyExpiry not supported by ActorSystemStub")

  def getLicenseKeyExpiry: Optional[LocalDate] =
    throw new UnsupportedOperationException("getLicenseKeyExpiry not supported by ActorSystemStub")
}

@InternalApi private[akka] object ActorSystemStub {
  object config {
    // this is backward compatible with the old behaviour, hence it uses the loader used to load the test-kit
    // which is not necessarily the one used to load the tests...
    // hence this might not include reference config related to the actually executing test
    //todo: might be better NOT to pass any class loader and let typesafeConfig rely on the contextClassLoader
    // (which is usually the system class loader)
    def defaultReference: Config = ConfigFactory.defaultReference(getClass.getClassLoader)
  }
}
