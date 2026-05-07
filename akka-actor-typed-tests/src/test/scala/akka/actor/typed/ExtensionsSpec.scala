/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future

import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.BootstrapSetup
import akka.actor.setup.ActorSystemSetup
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.Behaviors

class DummyExtension1 extends Extension
object DummyExtension1 extends ExtensionId[DummyExtension1] {
  def createExtension(system: ActorSystem[_]) = new DummyExtension1
  def get(system: ActorSystem[_]): DummyExtension1 = apply(system)
}
class DummyExtension1Setup(factory: ActorSystem[_] => DummyExtension1)
    extends AbstractExtensionSetup[DummyExtension1](DummyExtension1, factory)

class DummyExtension1ViaSetup extends DummyExtension1

class SlowExtension extends Extension
object SlowExtension extends ExtensionId[SlowExtension] {
  def createExtension(system: ActorSystem[_]) = {
    Thread.sleep(25)
    new SlowExtension
  }
}

class FailingToLoadExtension extends Extension
object FailingToLoadExtension extends ExtensionId[FailingToLoadExtension] {
  def createExtension(system: ActorSystem[_]) = {
    throw new RuntimeException("I cannot be trusted!")
  }
}

class MultiExtension(val n: Int) extends Extension
class MultiExtensionId(n: Int) extends ExtensionId[MultiExtension] {
  def createExtension(system: ActorSystem[_]): MultiExtension = new MultiExtension(n)
}

object InstanceCountingExtension extends ExtensionId[DummyExtension1] {
  val createCount = new AtomicInteger(0)
  override def createExtension(system: ActorSystem[_]): DummyExtension1 = {
    createCount.addAndGet(1)
    new DummyExtension1
  }
}

object AccessSystemFromConstructorExtensionId extends ExtensionId[AccessSystemFromConstructor] {
  override def createExtension(system: ActorSystem[_]): AccessSystemFromConstructor =
    new AccessSystemFromConstructor(system)
}
class AccessSystemFromConstructor(system: ActorSystem[_]) extends Extension {
  system.log.info("I log from the constructor")
  system.receptionist ! Receptionist.Find(ServiceKey[String]("i-just-made-it-up"), system.deadLetters) // or touch the receptionist!
}

object ExtensionsSpec {
  val config = ConfigFactory.parseString("""
akka.actor.typed {
  library-extensions += "akka.actor.typed.InstanceCountingExtension"
  }
   """).resolve()
}

class ExtensionsSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  "The extensions subsystem" must {
    "return the same instance for the same id" in
    withEmptyActorSystem("ExtensionsSpec01") { sys =>
      val instance1 = sys.registerExtension(DummyExtension1)
      val instance2 = sys.registerExtension(DummyExtension1)

      (instance1 should be).theSameInstanceAs(instance2)

      val instance3 = DummyExtension1(sys)
      (instance3 should be).theSameInstanceAs(instance2)

      val instance4 = DummyExtension1.get(sys)
      (instance4 should be).theSameInstanceAs(instance3)
    }

    "return the same instance for the same id concurrently" in
    withEmptyActorSystem("ExtensionsSpec02") { sys =>
      // not exactly water tight but better than nothing
      import sys.executionContext
      val futures = (0 to 1000).map(_ =>
        Future {
          sys.registerExtension(SlowExtension)
        })

      val instances = Future.sequence(futures).futureValue

      instances.reduce { (a, b) =>
        (a should be).theSameInstanceAs(b)
        b
      }
    }

    "load extensions from the configuration" in
    withEmptyActorSystem(
      "ExtensionsSpec03",
      Some(
        ConfigFactory.parseString(
          """
          akka.actor.typed.extensions = ["akka.actor.typed.DummyExtension1$", "akka.actor.typed.SlowExtension$"]
        """))) { sys =>
      sys.hasExtension(DummyExtension1) should ===(true)
      sys.extension(DummyExtension1) shouldBe a[DummyExtension1]

      sys.hasExtension(SlowExtension) should ===(true)
      sys.extension(SlowExtension) shouldBe a[SlowExtension]
    }

    "handle extensions that fail to initialize" in {
      def create(): Unit = {
        ActorSystem[Any](
          Behaviors.empty[Any],
          "ExtensionsSpec04",
          ConfigFactory.parseString("""
          akka.actor.typed.extensions = ["akka.actor.typed.FailingToLoadExtension$"]
        """))
      }

      intercept[RuntimeException] {
        create()
      }
      // and keeps happening (a retry to create on each access)
      intercept[RuntimeException] {
        create()
      }
    }

    "support multiple instances of the same type of extension (with different ids)" in
    withEmptyActorSystem("ExtensionsSpec06") { sys =>
      val id1 = new MultiExtensionId(1)
      val id2 = new MultiExtensionId(2)

      sys.registerExtension(id1).n should ===(1)
      sys.registerExtension(id2).n should ===(2)
      sys.registerExtension(id1).n should ===(1)
    }

    "allow for auto-loading of library-extensions" in
    withEmptyActorSystem("ExtensionsSpec06") { sys =>
      val listedExtensions = sys.settings.config.getStringList("akka.actor.typed.library-extensions")
      listedExtensions.size should be > 0
      // could be initialized by other tests, so at least once
      InstanceCountingExtension.createCount.get() should be > 0
    }

    "fail the system if a library-extension cannot be loaded" in
    intercept[RuntimeException] {
      withEmptyActorSystem(
        "ExtensionsSpec07",
        Some(
          ConfigFactory.parseString(
            """akka.actor.typed.library-extensions += "akka.actor.typed.FailingToLoadExtension$""""))) { _ =>
        ()
      }
    }

    "fail the system if a library-extension is missing" in
    intercept[RuntimeException] {
      withEmptyActorSystem(
        "ExtensionsSpec08",
        Some(ConfigFactory.parseString(
          """akka.actor.typed.library-extensions += "akka.actor.typed.MissingExtension""""))) { _ =>
        ()
      }
    }

    "load an extension implemented in Java" in
    withEmptyActorSystem("ExtensionsSpec09") { sys =>
      // no way to make apply work cleanly with extensions implemented in Java
      val instance1 = ExtensionsTest.MyExtension.get(sys)
      val instance2 = ExtensionsTest.MyExtension.get(sys)

      (instance1 should be).theSameInstanceAs(instance2)
    }

    "load registered extensions eagerly even for classic system" in {
      import akka.actor.typed.scaladsl.adapter._
      val beforeCreation = InstanceCountingExtension.createCount.get()
      val classicSystem = akka.actor.ActorSystem("as", ExtensionsSpec.config)
      try {
        val before = InstanceCountingExtension.createCount.get()
        InstanceCountingExtension(classicSystem.toTyped)
        val after = InstanceCountingExtension.createCount.get()

        // should have been loaded even before it was accessed in the test because InstanceCountingExtension is listed
        // as a typed library-extension in the config
        before shouldEqual beforeCreation + 1
        after shouldEqual before
      } finally {
        classicSystem.terminate().futureValue
      }
    }

    "not create an extension multiple times when using the ActorSystemAdapter" in {
      import akka.actor.typed.scaladsl.adapter._
      val classicSystem = akka.actor.ActorSystem()
      try {
        val ext1 = DummyExtension1(classicSystem.toTyped)
        val ext2 = DummyExtension1(classicSystem.toTyped)

        (ext1 should be).theSameInstanceAs(ext2)

      } finally {
        classicSystem.terminate().futureValue
      }
    }

    "override extensions via ActorSystemSetup" in
    withEmptyActorSystem(
      "ExtensionsSpec10",
      Some(
        ConfigFactory.parseString(
          """
          akka.actor.typed.extensions = ["akka.actor.typed.DummyExtension1$", "akka.actor.typed.SlowExtension$"]
        """)),
      Some(ActorSystemSetup(new DummyExtension1Setup(_ => new DummyExtension1ViaSetup)))) { sys =>
      sys.hasExtension(DummyExtension1) should ===(true)
      sys.extension(DummyExtension1) shouldBe a[DummyExtension1ViaSetup]
      DummyExtension1(sys) shouldBe a[DummyExtension1ViaSetup]
      DummyExtension1(sys) shouldBe theSameInstanceAs(DummyExtension1(sys))

      sys.hasExtension(SlowExtension) should ===(true)
      sys.extension(SlowExtension) shouldBe a[SlowExtension]
    }

    "allow for interaction with log from extension constructor" in {
      withEmptyActorSystem(
        "ExtensionsSpec11",
        Some(
          ConfigFactory.parseString(
            """
          akka.actor.typed.extensions = ["akka.actor.typed.AccessSystemFromConstructorExtensionId$"]
        """)),
        None) { sys =>
        AccessSystemFromConstructorExtensionId(sys) // would throw if it couldn't
      }
    }
  }

  def withEmptyActorSystem[T](name: String, config: Option[Config] = None, setup: Option[ActorSystemSetup] = None)(
      f: ActorSystem[_] => T): T = {

    val bootstrap = config match {
      case Some(c) => BootstrapSetup(c)
      case None    => BootstrapSetup(ExtensionsSpec.config)
    }
    val sys = setup match {
      case None    => ActorSystem[Any](Behaviors.empty[Any], name, bootstrap)
      case Some(s) =>
        // explicit Props.empty: https://github.com/lampepfl/dotty/issues/12679
        ActorSystem[Any](Behaviors.empty[Any], name, s.and(bootstrap), Props.empty)
    }

    try f(sys)
    finally {
      sys.terminate()
      sys.whenTerminated.futureValue
    }
  }
}
