/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.event

import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.BeforeAndAfterEach

import akka.actor.{ Actor, ActorRef, ActorSystem, PoisonPill, Props }
import akka.japi.Procedure
import akka.testkit._

object EventBusSpec {
  class TestActorWrapperActor(testActor: ActorRef) extends Actor {
    def receive = {
      case x => testActor.forward(x)
    }
  }
}

abstract class EventBusSpec(busName: String, conf: Config = ConfigFactory.empty())
    extends AkkaSpec(conf)
    with BeforeAndAfterEach {
  type BusType <: EventBus

  def createNewEventBus(): BusType

  final val bus = createNewEventBus()

  def createEvents(numberOfEvents: Int): Iterable[bus.Event]

  def createSubscriber(pipeTo: ActorRef): bus.Subscriber

  def classifierFor(event: bus.Event): bus.Classifier

  def disposeSubscriber(system: ActorSystem, subscriber: bus.Subscriber): Unit

  busName must {
    def createNewSubscriber() = createSubscriber(testActor).asInstanceOf[bus.Subscriber]
    def getClassifierFor(event: bus.Event) = classifierFor(event).asInstanceOf[bus.Classifier]
    def createNewEvents(numberOfEvents: Int): Iterable[bus.Event] =
      createEvents(numberOfEvents).asInstanceOf[Iterable[bus.Event]]

    val events = createNewEvents(100)
    val event = events.head
    val classifier = getClassifierFor(event)
    val subscriber = createNewSubscriber()

    "allow subscribers" in {
      bus.subscribe(subscriber, classifier) should ===(true)
    }

    "allow to unsubscribe already existing subscriber" in {
      bus.unsubscribe(subscriber, classifier) should ===(true)
    }

    "not allow to unsubscribe non-existing subscriber" in {
      val sub = createNewSubscriber()
      bus.unsubscribe(sub, classifier) should ===(false)
      disposeSubscriber(system, sub)
    }

    "not allow for the same subscriber to subscribe to the same channel twice" in {
      bus.subscribe(subscriber, classifier) should ===(true)
      bus.subscribe(subscriber, classifier) should ===(false)
      bus.unsubscribe(subscriber, classifier) should ===(true)
    }

    "not allow for the same subscriber to unsubscribe to the same channel twice" in {
      bus.subscribe(subscriber, classifier) should ===(true)
      bus.unsubscribe(subscriber, classifier) should ===(true)
      bus.unsubscribe(subscriber, classifier) should ===(false)
    }

    "allow to add multiple subscribers" in {
      val subscribers = (1 to 10).map { _ =>
        createNewSubscriber()
      }
      val events = createEvents(10)
      val classifiers = events.map(getClassifierFor)
      subscribers.zip(classifiers).forall { case (s, c) => bus.subscribe(s, c) } should ===(true)
      subscribers.zip(classifiers).forall { case (s, c) => bus.unsubscribe(s, c) } should ===(true)

      subscribers.foreach(disposeSubscriber(system, _))
    }

    "publishing events without any subscribers shouldn't be a problem" in {
      bus.publish(event)
    }

    "publish the given event to the only subscriber" in {
      bus.subscribe(subscriber, classifier)
      bus.publish(event)
      expectMsg(event)
      expectNoMessage()
      bus.unsubscribe(subscriber, classifier)
    }

    "publish to the only subscriber multiple times" in {
      bus.subscribe(subscriber, classifier)
      bus.publish(event)
      bus.publish(event)
      bus.publish(event)
      expectMsg(event)
      expectMsg(event)
      expectMsg(event)
      expectNoMessage()
      bus.unsubscribe(subscriber, classifier)
    }

    "publish the given event to all intended subscribers" in {
      val range = 0 until 10
      val subscribers = range.map(_ => createNewSubscriber())
      subscribers.foreach { s =>
        bus.subscribe(s, classifier) should ===(true)
      }
      bus.publish(event)
      range.foreach { _ =>
        expectMsg(event)
      }
      subscribers.foreach { s =>
        bus.unsubscribe(s, classifier) should ===(true); disposeSubscriber(system, s)
      }
    }

    "not publish the given event to any other subscribers than the intended ones" in {
      val otherSubscriber = createNewSubscriber()
      val otherClassifier = getClassifierFor(events.drop(1).head)
      bus.subscribe(subscriber, classifier)
      bus.subscribe(otherSubscriber, otherClassifier)
      bus.publish(event)
      expectMsg(event)
      bus.unsubscribe(subscriber, classifier)
      bus.unsubscribe(otherSubscriber, otherClassifier)
      expectNoMessage()
    }

    "not publish the given event to a former subscriber" in {
      bus.subscribe(subscriber, classifier)
      bus.unsubscribe(subscriber, classifier)
      bus.publish(event)
      expectNoMessage()
    }

    "cleanup subscriber" in {
      disposeSubscriber(system, subscriber)
    }
  }
}

object ActorEventBusSpec {
  class MyActorEventBus(protected val system: ActorSystem)
      extends ActorEventBus
      with ManagedActorClassification
      with ActorClassifier {

    type Event = Notification

    def classify(event: Event) = event.ref
    protected def mapSize = 32
    def publish(event: Event, subscriber: Subscriber) = subscriber ! event
  }

  case class Notification(ref: ActorRef, payload: Int)
}

class ActorEventBusSpec(conf: Config) extends EventBusSpec("ActorEventBus", conf) {
  import EventBusSpec.TestActorWrapperActor

  import akka.event.ActorEventBusSpec._

  def this() =
    this(ConfigFactory.parseString("akka.actor.debug.event-stream = on").withFallback(AkkaSpec.testConf))

  type BusType = MyActorEventBus
  def createNewEventBus(): BusType = new MyActorEventBus(system)

  // different actor in each event because we want each event to have a different classifier (see EventBusSpec tests)
  def createEvents(numberOfEvents: Int) = (0 until numberOfEvents).map(Notification(TestProbe().ref, _)).toSeq

  def createSubscriber(pipeTo: ActorRef) = system.actorOf(Props(new TestActorWrapperActor(pipeTo)))

  def classifierFor(event: BusType#Event) = event.ref

  def disposeSubscriber(system: ActorSystem, subscriber: BusType#Subscriber): Unit = system.stop(subscriber)

  // ManagedActorClassification specific tests

  "must unsubscribe subscriber when it terminates" in {
    val a1 = createSubscriber(system.deadLetters)
    val subs = createSubscriber(testActor)
    def m(i: Int) = Notification(a1, i)
    val p = TestProbe()
    system.eventStream.subscribe(p.ref, classOf[Logging.Debug])

    bus.subscribe(subs, a1)
    bus.publish(m(1))
    expectMsg(m(1))

    watch(subs)
    subs ! PoisonPill // when a1 dies, subs has nothing subscribed
    expectTerminated(subs)
    expectUnsubscribedByUnsubscriber(p, subs)

    bus.publish(m(2))
    expectNoMessage()

    disposeSubscriber(system, subs)
    disposeSubscriber(system, a1)
  }

  "must keep subscriber even if its subscription-actors have died" in {
    // Deaths of monitored actors should not influence the subscription.
    // For example: one might still want to monitor messages classified to A
    // even though it died, and handle these in some way.
    val a1 = createSubscriber(system.deadLetters)
    val subs = createSubscriber(testActor)
    def m(i: Int) = Notification(a1, i)

    bus.subscribe(subs, a1) should equal(true)

    bus.publish(m(1))
    expectMsg(m(1))

    watch(a1)
    a1 ! PoisonPill
    expectTerminated(a1)

    bus.publish(m(2)) // even though a1 has terminated, classification still applies
    expectMsg(m(2))

    disposeSubscriber(system, subs)
    disposeSubscriber(system, a1)
  }

  "must unregister subscriber only after it unsubscribes from all of it's subscriptions" in {
    val a1, a2 = createSubscriber(system.deadLetters)
    val subs = createSubscriber(testActor)
    def m1(i: Int) = Notification(a1, i)
    def m2(i: Int) = Notification(a2, i)

    val p = TestProbe()
    system.eventStream.subscribe(p.ref, classOf[Logging.Debug])

    bus.subscribe(subs, a1) should equal(true)
    bus.subscribe(subs, a2) should equal(true)

    bus.publish(m1(1))
    bus.publish(m2(1))
    expectMsg(m1(1))
    expectMsg(m2(1))

    bus.unsubscribe(subs, a1)
    bus.publish(m1(2))
    expectNoMessage()
    bus.publish(m2(2))
    expectMsg(m2(2))

    bus.unsubscribe(subs, a2)
    expectUnregisterFromUnsubscriber(p, subs)
    bus.publish(m1(3))
    bus.publish(m2(3))
    expectNoMessage()

    disposeSubscriber(system, subs)
    disposeSubscriber(system, a1)
    disposeSubscriber(system, a2)
  }

  private def expectUnsubscribedByUnsubscriber(p: TestProbe, a: ActorRef): Unit = {
    val expectedMsg = s"actor $a has terminated, unsubscribing it from $bus"
    p.fishForMessage(hint = expectedMsg) {
      case Logging.Debug(_, _, msg) if msg.equals(expectedMsg) => true
      case _                                                   => false
    }
  }

  private def expectUnregisterFromUnsubscriber(p: TestProbe, a: ActorRef): Unit = {
    val expectedMsg = s"unregistered watch of $a in $bus"
    p.fishForMessage(hint = expectedMsg) {
      case Logging.Debug(_, _, msg) if msg.equals(expectedMsg) => true
      case _                                                   => false
    }
  }
}

object ScanningEventBusSpec {

  class MyScanningEventBus extends EventBus with ScanningClassification {
    type Event = Int
    type Subscriber = Procedure[Int]
    type Classifier = String

    protected def compareClassifiers(a: Classifier, b: Classifier): Int = a.compareTo(b)
    protected def compareSubscribers(a: Subscriber, b: Subscriber): Int = akka.util.Helpers.compareIdentityHash(a, b)

    protected def matches(classifier: Classifier, event: Event): Boolean = event.toString == classifier

    protected def publish(event: Event, subscriber: Subscriber): Unit = subscriber(event)
  }
}

class ScanningEventBusSpec extends EventBusSpec("ScanningEventBus") {
  import ScanningEventBusSpec._

  type BusType = MyScanningEventBus

  def createNewEventBus(): BusType = new MyScanningEventBus

  def createEvents(numberOfEvents: Int) = (0 until numberOfEvents)

  def createSubscriber(pipeTo: ActorRef) = new Procedure[Int] { def apply(i: Int) = pipeTo ! i }

  def classifierFor(event: BusType#Event) = event.toString

  def disposeSubscriber(system: ActorSystem, subscriber: BusType#Subscriber): Unit = ()
}

object LookupEventBusSpec {
  class MyLookupEventBus extends EventBus with LookupClassification {
    type Event = Int
    type Subscriber = Procedure[Int]
    type Classifier = String

    override protected def classify(event: Int): String = event.toString
    override protected def compareSubscribers(a: Procedure[Int], b: Procedure[Int]): Int =
      akka.util.Helpers.compareIdentityHash(a, b)
    override protected def mapSize() = 32
    override protected def publish(event: Int, subscriber: Procedure[Int]): Unit =
      subscriber(event)
  }
}

class LookupEventBusSpec extends EventBusSpec("LookupEventBus") {
  import LookupEventBusSpec._

  type BusType = MyLookupEventBus

  def createNewEventBus(): BusType = new MyLookupEventBus

  def createEvents(numberOfEvents: Int) = (0 until numberOfEvents)

  def createSubscriber(pipeTo: ActorRef) = new Procedure[Int] { def apply(i: Int) = pipeTo ! i }

  def classifierFor(event: BusType#Event) = event.toString

  def disposeSubscriber(system: ActorSystem, subscriber: BusType#Subscriber): Unit = ()
}
