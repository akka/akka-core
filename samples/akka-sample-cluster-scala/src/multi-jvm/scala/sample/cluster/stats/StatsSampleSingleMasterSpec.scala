package sample.cluster.stats

import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.Routers
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.Props
import akka.actor.typed.SpawnProtocol
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.typed.ClusterSingleton
import akka.cluster.typed.ClusterSingletonSettings
import akka.cluster.typed.SingletonActor
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.Future

object StatsSampleSingleMasterSpecConfig extends MultiNodeConfig {
  // register the named roles (nodes) of the test
  // note that this is not the same thing as cluster node roles
  val first = role("first")
  val second = role("second")
  val third = role("third")

  // this configuration will be used for all nodes
  // note that no fixed host names and ports are used
  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = cluster
    akka.cluster.roles = [compute]
    """).withFallback(ConfigFactory.load()))

}

// need one concrete test class per node
class StatsSampleSingleMasterSpecMultiJvmNode1 extends StatsSampleSingleMasterSpec
class StatsSampleSingleMasterSpecMultiJvmNode2 extends StatsSampleSingleMasterSpec
class StatsSampleSingleMasterSpecMultiJvmNode3 extends StatsSampleSingleMasterSpec

abstract class StatsSampleSingleMasterSpec extends MultiNodeSpec(StatsSampleSingleMasterSpecConfig)
  with WordSpecLike with Matchers with BeforeAndAfterAll with ImplicitSender {

  import StatsSampleSingleMasterSpecConfig._

  override def initialParticipants = roles.size

  override def beforeAll() = multiNodeSpecBeforeAll()

  override def afterAll() = multiNodeSpecAfterAll()

  implicit val typedSystem = system.toTyped

  var singletonProxy: ActorRef[StatsService.Command] = _

  "The stats sample with single master" must {
    "illustrate how to startup cluster" in within(15.seconds) {
      Cluster(system).subscribe(testActor, classOf[MemberUp])
      expectMsgClass(classOf[CurrentClusterState])

      val firstAddress = node(first).address
      val secondAddress = node(second).address
      val thirdAddress = node(third).address

      Cluster(system) join firstAddress

      receiveN(3).collect { case MemberUp(m) => m.address }.toSet should be(
        Set(firstAddress, secondAddress, thirdAddress))

      Cluster(system).unsubscribe(testActor)

      val singletonSettings = ClusterSingletonSettings(typedSystem).withRole("compute")
      singletonProxy = ClusterSingleton(typedSystem).init(
        SingletonActor(
          Behaviors.setup[StatsService.Command] { ctx =>
            // just run some local workers for this test
            val workersRouter = ctx.spawn(Routers.pool(2)(StatsWorker()), "WorkersRouter")
            StatsService(workersRouter)
          },
          "StatsService",
        ).withSettings(singletonSettings)
      )

      testConductor.enter("all-up")
    }

    "show usage of the statsServiceProxy" in within(20.seconds) {
      // eventually the service should be ok,
      // service and worker nodes might not be up yet
      awaitAssert {
        system.log.info("Trying a request")
        val probe = TestProbe[StatsService.Response]()
        singletonProxy ! StatsService.ProcessText("this is the text that will be analyzed", probe.ref)
        val response = probe.expectMessageType[StatsService.JobResult](3.seconds)
        response.meanWordLength should be(3.875 +- 0.001)
      }

      testConductor.enter("done")
    }
  }

}
