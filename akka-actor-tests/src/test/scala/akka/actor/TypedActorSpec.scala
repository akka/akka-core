/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.actor

import java.util.concurrent.{ CountDownLatch, TimeUnit, TimeoutException }
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach }

import akka.actor.TypedActor._
import akka.japi.{ Option => JOption }
import akka.pattern.ask
import akka.routing.RoundRobinGroup
import akka.serialization.{ JavaSerializer, SerializerWithStringManifest }
import akka.testkit.{ filterEvents, AkkaSpec, DefaultTimeout, EventFilter, TimingTest }
import akka.util.Timeout

object TypedActorSpec {

  @nowarn
  val config = """
    pooled-dispatcher {
      type = "akka.dispatch.BalancingDispatcherConfigurator"
      executor = "thread-pool-executor"
      thread-pool-executor {
        fixed-pool-size = 60
      }
    }
    akka.actor.serializers.sample = "akka.actor.TypedActorSpec$SampleSerializerWithStringManifest$"
    akka.actor.serialization-bindings."akka.actor.TypedActorSpec$WithStringSerializedClass" = sample
    # test is using Java serialization and not priority to convert
    akka.actor.allow-java-serialization = on
    akka.actor.warn-about-java-serializer-usage = off
    """

  class CyclicIterator[T](val items: immutable.Seq[T]) extends Iterator[T] {

    private[this] val current = new AtomicReference(items)

    def hasNext = items != Nil

    def next(): T = {
      @tailrec
      def findNext(): T = {
        val currentItems = current.get
        val newItems = currentItems match {
          case Nil => items
          case xs  => xs
        }

        if (current.compareAndSet(currentItems, newItems.tail)) newItems.head
        else findNext()
      }

      findNext()
    }

    override def exists(f: T => Boolean): Boolean = items.exists(f)
  }

  trait Foo {
    def pigdog(): String

    @nowarn
    @throws(classOf[TimeoutException])
    def self = akka.actor.TypedActor.self[Foo]

    def futurePigdog(): Future[String]

    def futurePigdog(delay: FiniteDuration): Future[String]

    def futurePigdog(delay: FiniteDuration, numbered: Int): Future[String]

    def futureComposePigdogFrom(foo: Foo): Future[String]

    def failingFuturePigdog(): Future[String] = throw new IllegalStateException("expected")

    @throws(classOf[TimeoutException])
    def failingOptionPigdog(): Option[String] = throw new IllegalStateException("expected")

    @throws(classOf[TimeoutException])
    def failingJOptionPigdog(): JOption[String] = throw new IllegalStateException("expected")

    def failingPigdog(): Unit = throw new IllegalStateException("expected")

    @throws(classOf[TimeoutException])
    def optionPigdog(): Option[String]

    @throws(classOf[TimeoutException])
    def optionPigdog(delay: FiniteDuration): Option[String]

    @throws(classOf[TimeoutException])
    def joptionPigdog(delay: FiniteDuration): JOption[String]

    def nullFuture(): Future[Any] = null

    def nullJOption(): JOption[Any] = null

    def nullOption(): Option[Any] = null

    def nullReturn(): Any = null

    def incr(): Unit

    @throws(classOf[TimeoutException])
    def read(): Int

    def testMethodCallSerialization(foo: Foo, s: String, i: Int, o: WithStringSerializedClass): Unit =
      throw new IllegalStateException(s"expected $foo $s $i $o")
  }

  class Bar extends Foo with Serializable {

    import akka.actor.TypedActor.dispatcher

    def pigdog() = "Pigdog"

    def futurePigdog(): Future[String] = Future.successful(pigdog())

    def futurePigdog(delay: FiniteDuration): Future[String] = {
      Thread.sleep(delay.toMillis)
      futurePigdog()
    }

    def futurePigdog(delay: FiniteDuration, numbered: Int): Future[String] = {
      Thread.sleep(delay.toMillis)
      Future.successful(pigdog() + numbered)
    }

    @nowarn
    def futureComposePigdogFrom(foo: Foo): Future[String] = {
      foo.futurePigdog(500 millis).map(_.toUpperCase)
    }

    def optionPigdog(): Option[String] = Some(pigdog())

    def optionPigdog(delay: FiniteDuration): Option[String] = {
      Thread.sleep(delay.toMillis)
      Some(pigdog())
    }

    def joptionPigdog(delay: FiniteDuration): JOption[String] = {
      Thread.sleep(delay.toMillis)
      JOption.some(pigdog())
    }

    var internalNumber = 0

    def incr(): Unit = {
      internalNumber += 1
    }

    def read() = internalNumber
  }

  trait Stackable1 {
    def stackable1: String = "foo"
  }

  trait Stackable2 {
    def stackable2: String = "bar"
  }

  trait Stacked extends Stackable1 with Stackable2 {
    def stacked: String = stackable1 + stackable2

    def notOverriddenStacked: String = stackable1 + stackable2
  }

  class StackedImpl extends Stacked {
    override def stacked: String = "FOOBAR" //Uppercase
  }

  trait LifeCycles {
    def crash(): Unit
  }

  @nowarn("msg=deprecated")
  class LifeCyclesImpl(val latch: CountDownLatch)
      extends PreStart
      with PostStop
      with PreRestart
      with PostRestart
      with LifeCycles
      with Receiver {

    @nowarn
    private def ensureContextAvailable[T](f: => T): T = akka.actor.TypedActor.context match {
      case null => throw new IllegalStateException("TypedActor.context is null!")
      case _    => f
    }

    override def crash(): Unit = throw new IllegalStateException("Crash!")

    override def preStart(): Unit = ensureContextAvailable(latch.countDown())

    override def postStop(): Unit = ensureContextAvailable(for (_ <- 1 to 3) latch.countDown())

    override def preRestart(reason: Throwable, message: Option[Any]): Unit =
      ensureContextAvailable(for (_ <- 1 to 5) latch.countDown())

    override def postRestart(reason: Throwable): Unit = ensureContextAvailable(for (_ <- 1 to 7) latch.countDown())

    override def onReceive(msg: Any, sender: ActorRef): Unit = {
      ensureContextAvailable(msg match {
        case "pigdog" => sender ! "dogpig"
        case _        =>
      })
    }
  }

  trait F {
    def f(pow: Boolean): Int
  }

  class FI extends F {
    def f(pow: Boolean): Int = if (pow) throw new IllegalStateException("expected") else 1
  }

  object SampleSerializerWithStringManifest extends SerializerWithStringManifest {

    val manifest = "M"

    override def identifier: Int = 777

    override def manifest(o: AnyRef): String = manifest

    override def toBinary(o: AnyRef): Array[Byte] = o match {
      case _: WithStringSerializedClass => Array(255.toByte)
      case _                            => throw new IllegalArgumentException(s"Cannot serialize object of type [${o.getClass.getName}]")
    }

    override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
      case _ if bytes.length == 1 && bytes(0) == 255.toByte => WithStringSerializedClass()
      case _                                                => throw new IllegalArgumentException(s"Cannot deserialize object with manifest $manifest")
    }
  }

  case class WithStringSerializedClass()

}

@nowarn
class TypedActorSpec
    extends AkkaSpec(TypedActorSpec.config)
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with DefaultTimeout {

  import akka.actor.TypedActorSpec._

  def newFooBar: Foo = newFooBar(timeout.duration)

  def newFooBar(d: FiniteDuration): Foo =
    akka.actor.TypedActor(system).typedActorOf(TypedProps[Bar](classOf[Foo], classOf[Bar]).withTimeout(Timeout(d)))

  def newFooBar(dispatcher: String, d: FiniteDuration): Foo =
    akka.actor
      .TypedActor(system)
      .typedActorOf(TypedProps[Bar](classOf[Foo], classOf[Bar]).withTimeout(Timeout(d)).withDispatcher(dispatcher))

  def newStacked(): Stacked =
    akka.actor
      .TypedActor(system)
      .typedActorOf(TypedProps[StackedImpl](classOf[Stacked], classOf[StackedImpl]).withTimeout(timeout))

  def mustStop(typedActor: AnyRef) = akka.actor.TypedActor(system).stop(typedActor) should ===(true)

  "TypedActors" must {

    "be able to instantiate" in {
      val t = newFooBar
      akka.actor.TypedActor(system).isTypedActor(t) should ===(true)
      mustStop(t)
    }

    "be able to stop" in {
      val t = newFooBar
      mustStop(t)
    }

    "not stop non-started ones" in {
      akka.actor.TypedActor(system).stop(null) should ===(false)
    }

    "throw an IllegalStateException when TypedActor.self is called in the wrong scope" in {
      filterEvents(EventFilter[IllegalStateException]("Calling")) {
        intercept[IllegalStateException] {
          akka.actor.TypedActor.self[Foo]
        }.getMessage should ===("Calling TypedActor.self outside of a TypedActor implementation method!")
      }
    }

    "have access to itself when executing a method call" in {
      val t = newFooBar
      t.self should ===(t)
      mustStop(t)
    }

    "be able to call toString" in {
      val t = newFooBar
      t.toString should ===(akka.actor.TypedActor(system).getActorRefFor(t).toString)
      mustStop(t)
    }

    "be able to call equals" in {
      val t = newFooBar
      t should ===(t)
      (t should not).equal(null)
      mustStop(t)
    }

    "be able to call hashCode" in {
      val t = newFooBar
      t.hashCode should ===(akka.actor.TypedActor(system).getActorRefFor(t).hashCode)
      mustStop(t)
    }

    "be able to call user-defined void-methods" in {
      val t = newFooBar
      t.incr()
      t.read() should ===(1)
      t.incr()
      t.read() should ===(2)
      t.read() should ===(2)
      mustStop(t)
    }

    "be able to call normally returning methods" in {
      val t = newFooBar
      t.pigdog() should ===("Pigdog")
      mustStop(t)
    }

    "be able to call null returning methods" in {
      val t = newFooBar
      t.nullJOption() should be(JOption.none)
      t.nullOption() should ===(None)
      t.nullReturn() should ===(null)
      Await.result(t.nullFuture(), timeout.duration) should ===(null)
    }

    "be able to call Future-returning methods non-blockingly" in {
      val t = newFooBar
      val f = t.futurePigdog(200 millis)
      f.isCompleted should ===(false)
      Await.result(f, timeout.duration) should ===("Pigdog")
      mustStop(t)
    }

    "be able to call multiple Future-returning methods non-blockingly" in within(timeout.duration) {
      val t = newFooBar
      val futures = for (i <- 1 to 20) yield (i, t.futurePigdog(20 millis, i))
      for ((i, f) <- futures) {
        Await.result(f, remaining) should ===("Pigdog" + i)
      }
      mustStop(t)
    }

    "be able to call methods returning Java Options" taggedAs TimingTest in {
      val t = newFooBar(1 second)
      t.joptionPigdog(100 millis).get should ===("Pigdog")
      t.joptionPigdog(2 seconds) should ===(JOption.none[String])
      mustStop(t)
    }

    "be able to handle AskTimeoutException as None" taggedAs TimingTest in {
      val t = newFooBar(200 millis)
      t.joptionPigdog(600 millis) should ===(JOption.none[String])
      mustStop(t)
    }

    "be able to call methods returning Scala Options" taggedAs TimingTest in {
      val t = newFooBar(1 second)
      t.optionPigdog(100 millis).get should ===("Pigdog")
      t.optionPigdog(2 seconds) should ===(None)
      mustStop(t)
    }

    "be able to compose futures without blocking" in within(timeout.duration) {
      val t, t2 = newFooBar(remaining)
      val f = t.futureComposePigdogFrom(t2)
      f.isCompleted should ===(false)
      Await.result(f, remaining) should ===("PIGDOG")
      mustStop(t)
      mustStop(t2)
    }

    "be able to handle exceptions when calling methods" in {
      filterEvents(EventFilter[IllegalStateException]("expected")) {
        val boss = system.actorOf(Props(new Actor {
          override val supervisorStrategy = OneForOneStrategy() {
            case e: IllegalStateException if e.getMessage == "expected" => SupervisorStrategy.Resume
          }
          def receive = {
            case p: TypedProps[_] => context.sender() ! akka.actor.TypedActor(context).typedActorOf(p)
          }
        }))
        val t = Await.result(
          (boss ? TypedProps[Bar](classOf[Foo], classOf[Bar]).withTimeout(2 seconds)).mapTo[Foo],
          timeout.duration)

        t.incr()
        t.failingPigdog()
        t.read() should ===(1) //Make sure state is not reset after failure

        intercept[IllegalStateException] { Await.result(t.failingFuturePigdog(), 2 seconds) }.getMessage should ===(
          "expected")
        t.read() should ===(1) //Make sure state is not reset after failure

        intercept[IllegalStateException] { t.failingJOptionPigdog() }.getMessage should ===("expected")
        t.read() should ===(1) //Make sure state is not reset after failure

        intercept[IllegalStateException] { t.failingOptionPigdog() }.getMessage should ===("expected")

        t.read() should ===(1) //Make sure state is not reset after failure

        mustStop(t)
      }
    }

    "be restarted on failure" in {
      filterEvents(EventFilter[IllegalStateException]("expected")) {
        val t = newFooBar(Duration(2, "s"))
        intercept[IllegalStateException] { t.failingOptionPigdog() }.getMessage should ===("expected")
        t.optionPigdog() should ===(Some("Pigdog"))
        mustStop(t)

        val ta: F = akka.actor.TypedActor(system).typedActorOf(TypedProps[FI]())
        intercept[IllegalStateException] { ta.f(true) }.getMessage should ===("expected")
        ta.f(false) should ===(1)

        mustStop(ta)
      }
    }

    "be able to support stacked traits for the interface part" in {
      val t = newStacked()
      t.notOverriddenStacked should ===("foobar")
      t.stacked should ===("FOOBAR")
      mustStop(t)
    }

    "be able to support implementation only typed actors" in within(timeout.duration) {
      val t: Foo = akka.actor.TypedActor(system).typedActorOf(TypedProps[Bar]())
      val f = t.futurePigdog(200 millis)
      val f2 = t.futurePigdog(Duration.Zero)
      f2.isCompleted should ===(false)
      f.isCompleted should ===(false)
      Await.result(f, remaining) should ===(Await.result(f2, remaining))
      mustStop(t)
    }

    "be able to support implementation only typed actors with complex interfaces" in {
      val t: Stackable1 with Stackable2 = akka.actor.TypedActor(system).typedActorOf(TypedProps[StackedImpl]())
      t.stackable1 should ===("foo")
      t.stackable2 should ===("bar")
      mustStop(t)
    }

    "be able to use balancing dispatcher" in within(timeout.duration) {
      val thais = for (_ <- 1 to 60) yield newFooBar("pooled-dispatcher", 6 seconds)
      val iterator = new CyclicIterator(thais)

      val results = for (i <- 1 to 120) yield (i, iterator.next().futurePigdog(200 millis, i))

      for ((i, r) <- results) Await.result(r, remaining) should ===("Pigdog" + i)

      for (t <- thais) mustStop(t)
    }

    "be able to serialize and deserialize invocations" in {
      import java.io._
      JavaSerializer.currentSystem.withValue(system.asInstanceOf[ExtendedActorSystem]) {
        val m = akka.actor.TypedActor.MethodCall(classOf[Foo].getDeclaredMethod("pigdog"), Array[AnyRef]())
        val baos = new ByteArrayOutputStream(8192 * 4)
        val out = new ObjectOutputStream(baos)

        out.writeObject(m)
        out.close()

        val in = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))

        val mNew = in.readObject().asInstanceOf[akka.actor.TypedActor.MethodCall]

        mNew.method should ===(m.method)
      }
    }

    "be able to serialize and deserialize invocations' parameters" in {
      import java.io._
      val someFoo: Foo = new Bar
      JavaSerializer.currentSystem.withValue(system.asInstanceOf[ExtendedActorSystem]) {
        val m = akka.actor.TypedActor.MethodCall(
          classOf[Foo].getDeclaredMethod(
            "testMethodCallSerialization",
            Array[Class[_]](classOf[Foo], classOf[String], classOf[Int], classOf[WithStringSerializedClass]): _*),
          Array[AnyRef](someFoo, null, 1.asInstanceOf[AnyRef], WithStringSerializedClass()))
        val baos = new ByteArrayOutputStream(8192 * 4)
        val out = new ObjectOutputStream(baos)

        out.writeObject(m)
        out.close()

        val in = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))

        val mNew = in.readObject().asInstanceOf[akka.actor.TypedActor.MethodCall]

        mNew.method should ===(m.method)
        mNew.parameters should have size 4
        mNew.parameters(0) should not be null
        mNew.parameters(0).getClass should ===(classOf[Bar])
        mNew.parameters(1) should ===(null)
        mNew.parameters(2) should not be null
        mNew.parameters(2).asInstanceOf[Int] should ===(1)
        mNew.parameters(3) should not be null
        mNew.parameters(3).asInstanceOf[WithStringSerializedClass] should ===(WithStringSerializedClass())
      }
    }

    "be able to serialize and deserialize proxies" in {
      import java.io._
      JavaSerializer.currentSystem.withValue(system.asInstanceOf[ExtendedActorSystem]) {
        val t = newFooBar(Duration(2, "s"))

        t.optionPigdog() should ===(Some("Pigdog"))

        val baos = new ByteArrayOutputStream(8192 * 4)
        val out = new ObjectOutputStream(baos)

        out.writeObject(t)
        out.close()

        val in = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))

        val tNew = in.readObject().asInstanceOf[Foo]

        tNew should ===(t)

        tNew.optionPigdog() should ===(Some("Pigdog"))

        mustStop(t)
      }
    }

    "be able to override lifecycle callbacks" in {
      val latch = new CountDownLatch(16)
      val ta = akka.actor.TypedActor(system)
      val t: LifeCycles = ta.typedActorOf(TypedProps[LifeCyclesImpl](classOf[LifeCycles], new LifeCyclesImpl(latch)))
      EventFilter[IllegalStateException]("Crash!", occurrences = 1).intercept {
        t.crash()
      }

      //Sneak in a check for the Receiver override
      val ref = ta.getActorRefFor(t)

      ref.tell("pigdog", testActor)

      expectMsg(timeout.duration, "dogpig")

      //Done with that now

      ta.poisonPill(t)
      latch.await(10, TimeUnit.SECONDS) should ===(true)
    }
  }
}

@nowarn
class TypedActorRouterSpec
    extends AkkaSpec(TypedActorSpec.config)
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with DefaultTimeout {

  import akka.actor.TypedActorSpec._

  def newFooBar: Foo = newFooBar(timeout.duration)

  def newFooBar(d: FiniteDuration): Foo =
    akka.actor.TypedActor(system).typedActorOf(TypedProps[Bar](classOf[Foo], classOf[Bar]).withTimeout(Timeout(d)))

  def mustStop(typedActor: AnyRef) = akka.actor.TypedActor(system).stop(typedActor) should ===(true)

  "TypedActor Router" must {

    "work" in {
      val t1 = newFooBar
      val t2 = newFooBar
      val t3 = newFooBar
      val t4 = newFooBar
      val routees = List(t1, t2, t3, t4).map { t =>
        akka.actor.TypedActor(system).getActorRefFor(t).path.toStringWithoutAddress
      }

      akka.actor.TypedActor(system).isTypedActor(t1) should ===(true)
      akka.actor.TypedActor(system).isTypedActor(t2) should ===(true)

      val router = system.actorOf(RoundRobinGroup(routees).props(), "router")

      val typedRouter = akka.actor.TypedActor(system).typedActorOf[Foo, Foo](TypedProps[Foo](), router)

      info("got = " + typedRouter.optionPigdog())
      info("got = " + typedRouter.optionPigdog())
      info("got = " + typedRouter.optionPigdog())
      info("got = " + typedRouter.optionPigdog())
      info("got = " + typedRouter.optionPigdog())

      mustStop(t1)
      mustStop(t2)
      mustStop(t3)
      mustStop(t4)
    }
  }

}
