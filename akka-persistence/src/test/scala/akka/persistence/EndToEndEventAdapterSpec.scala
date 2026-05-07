/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import java.io.File

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor._
import akka.persistence.journal.{ EventAdapter, EventSeq }
import akka.testkit.TestProbe

object EndToEndEventAdapterSpec {

  trait AppModel { def payload: Any }
  case class A(payload: Any) extends AppModel
  case class B(payload: Any) extends AppModel
  case class NewA(payload: Any) extends AppModel
  case class NewB(payload: Any) extends AppModel

  case class JSON(payload: Any)

  class AEndToEndAdapter(@nowarn("msg=never used") system: ExtendedActorSystem) extends EventAdapter {
    override def manifest(event: Any): String = event.getClass.getCanonicalName

    override def toJournal(event: Any): Any =
      event match {
        case m: AppModel => JSON(m.payload)
        case _           => throw new RuntimeException()
      }
    override def fromJournal(event: Any, manifest: String): EventSeq = event match {
      case m: JSON if m.payload.toString.startsWith("a") => EventSeq.single(A(m.payload))
      case _                                             => EventSeq.empty
    }
  }
  class NewAEndToEndAdapter(@nowarn("msg=never used") system: ExtendedActorSystem) extends EventAdapter {
    override def manifest(event: Any): String = event.getClass.getCanonicalName

    override def toJournal(event: Any): Any =
      event match {
        case m: AppModel => JSON(m.payload)
        case _           => throw new RuntimeException()
      }
    override def fromJournal(event: Any, manifest: String): EventSeq = event match {
      case m: JSON if m.payload.toString.startsWith("a") => EventSeq.single(NewA(m.payload))
      case _                                             => EventSeq.empty
    }
  }
  class BEndToEndAdapter(@nowarn("msg=never used") system: ExtendedActorSystem) extends EventAdapter {
    override def manifest(event: Any): String = event.getClass.getCanonicalName

    override def toJournal(event: Any): Any =
      event match {
        case m: AppModel => JSON(m.payload)
        case _           => throw new RuntimeException()
      }
    override def fromJournal(event: Any, manifest: String): EventSeq = event match {
      case m: JSON if m.payload.toString.startsWith("b") => EventSeq.single(B(m.payload))
      case _                                             => EventSeq.empty
    }
  }
  class NewBEndToEndAdapter(@nowarn("msg=never used") system: ExtendedActorSystem) extends EventAdapter {
    override def manifest(event: Any): String = event.getClass.getCanonicalName

    override def toJournal(event: Any): Any =
      event match {
        case m: AppModel => JSON(m.payload)
        case _           => throw new RuntimeException()
      }
    override def fromJournal(event: Any, manifest: String): EventSeq = event match {
      case m: JSON if m.payload.toString.startsWith("b") => EventSeq.single(NewB(m.payload))
      case _                                             => EventSeq.empty
    }
  }

  class EndToEndAdapterActor(
      name: String,
      override val journalPluginId: String,
      @nowarn("msg=never used") probe: Option[ActorRef])
      extends NamedPersistentActor(name)
      with PersistentActor {

    var state: List[Any] = Nil

    val persistIncoming: Receive = {
      case GetState =>
        state.reverse.foreach { sender() ! _ }
      case in =>
        persist(in) { e =>
          state ::= e
          sender() ! e
        }
    }

    override def receiveRecover = {
      case RecoveryCompleted => // ignore
      case e                 => state ::= e
    }
    override def receiveCommand = persistIncoming

  }

}

// needs persistence between actor systems, thus not running with the inmem journal
// FIXME move to inmem + proxy
class EndToEndEventAdapterSpec extends AnyWordSpecLike with Matchers with BeforeAndAfterAll {
  import EndToEndEventAdapterSpec._

  val journalName = "leveldb"
  val journalConfig = PersistenceSpec.config("leveldb", "LeveldbEndToEndEventAdapterSpec")

  val storageLocations = List("akka.persistence.journal.leveldb.dir").map(s => new File(journalConfig.getString(s)))

  override protected def beforeAll(): Unit = {
    storageLocations.foreach(FileUtils.deleteDirectory)
  }

  override protected def afterAll(): Unit = {
    storageLocations.foreach(FileUtils.deleteDirectory)
  }

  val noAdaptersConfig = ConfigFactory.parseString("")

  val adaptersConfig = ConfigFactory.parseString(s"""
    |akka.persistence.journal {
    |  $journalName {
    |    event-adapters {
    |      a = "${classOf[EndToEndEventAdapterSpec].getCanonicalName}$$AEndToEndAdapter"
    |      b = "${classOf[EndToEndEventAdapterSpec].getCanonicalName}$$BEndToEndAdapter"
    |    }
    |    event-adapter-bindings {
    |      # to journal
    |      "${classOf[EndToEndEventAdapterSpec].getCanonicalName}$$A" = a
    |      "${classOf[EndToEndEventAdapterSpec].getCanonicalName}$$B" = b
    |      # from journal
    |      "${classOf[EndToEndEventAdapterSpec].getCanonicalName}$$JSON" = [a, b]
    |
    |    }
    |  }
    |}
    |akka.loggers = ["akka.testkit.TestEventListener"]
    """.stripMargin)

  val newAdaptersConfig = ConfigFactory.parseString(s"""
    |akka.persistence.journal {
    |  $journalName {
    |    event-adapters {
    |      a = "${classOf[EndToEndEventAdapterSpec].getCanonicalName}$$NewAEndToEndAdapter"
    |      b = "${classOf[EndToEndEventAdapterSpec].getCanonicalName}$$NewBEndToEndAdapter"
    |    }
    |    event-adapter-bindings {
    |      # to journal
    |      "${classOf[EndToEndEventAdapterSpec].getCanonicalName}$$A" = a
    |      "${classOf[EndToEndEventAdapterSpec].getCanonicalName}$$B" = b
    |      # from journal
    |      "${classOf[EndToEndEventAdapterSpec].getCanonicalName}$$JSON" = [a, b]
    |
    |    }
    |  }
    |}
    """.stripMargin)

  def persister(name: String, probe: Option[ActorRef] = None)(implicit system: ActorSystem) =
    system.actorOf(Props(classOf[EndToEndAdapterActor], name, "akka.persistence.journal." + journalName, probe))

  def withActorSystem[T](name: String, config: Config)(block: ActorSystem => T): T = {
    val system = ActorSystem(name, journalConfig.withFallback(config))
    try block(system)
    finally Await.ready(system.terminate(), 3.seconds)
  }

  "EventAdapters in end-to-end scenarios" must {

    "use the same adapter when reading as was used when writing to the journal" in
    withActorSystem("SimpleSystem", adaptersConfig) { implicit system =>
      val p = TestProbe()
      implicit val ref: ActorRef = p.ref

      val p1 = persister("p1")
      val a = A("a1")
      val b = B("b1")
      p1 ! a
      p1 ! b
      p.expectMsg(a)
      p.expectMsg(b)

      p.watch(p1)
      p1 ! PoisonPill
      p.expectTerminated(p1)

      val p11 = persister("p1")
      p11 ! GetState
      p.expectMsg(A("a1"))
      p.expectMsg(B("b1"))
    }

    "allow using an adapter, when write was performed without an adapter" in {
      val persistentName = "p2"

      withActorSystem("NoAdapterSystem", adaptersConfig) { implicit system =>
        val p = TestProbe()
        implicit val ref: ActorRef = p.ref

        val p2 = persister(persistentName)
        val a = A("a1")
        val b = B("b1")
        p2 ! a
        p2 ! b
        p.expectMsg(a)
        p.expectMsg(b)

        p.watch(p2)
        p2 ! PoisonPill
        p.expectTerminated(p2)

        val p11 = persister(persistentName)
        p11 ! GetState
        p.expectMsg(A("a1"))
        p.expectMsg(B("b1"))
      }

      withActorSystem("NowAdaptersAddedSystem", newAdaptersConfig) { implicit system =>
        val p = TestProbe()
        implicit val ref: ActorRef = p.ref

        val p22 = persister(persistentName)
        p22 ! GetState
        p.expectMsg(NewA("a1"))
        p.expectMsg(NewB("b1"))
      }
    }

    "give nice error message when unable to play back as adapter does not exist" in {
      // after some time, we start the system a-new...
      // and the adapter originally used for adapting A is missing from the configuration!
      val journalPath = s"akka.persistence.journal.$journalName"
      val missingAdapterConfig = adaptersConfig
        .withoutPath(s"$journalPath.event-adapters.a")
        .withoutPath(
          s"""$journalPath.event-adapter-bindings."${classOf[EndToEndEventAdapterSpec].getCanonicalName}$$A"""")

      withActorSystem("MissingAdapterSystem", journalConfig.withFallback(missingAdapterConfig)) { implicit system2 =>
        intercept[IllegalArgumentException] {
          Persistence(system2).adaptersFor(s"akka.persistence.journal.$journalName").get(classOf[String])
        }.getMessage should include("was bound to undefined event-adapter: a (bindings: [a, b], known adapters: b)")
      }
    }
  }
}
