/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.util.concurrent.Executors
import java.util.concurrent.TimeoutException

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.ConfigurationException
import akka.Done
import akka.actor.CoordinatedShutdown.Phase
import akka.actor.CoordinatedShutdown.UnknownReason
import akka.testkit.AkkaSpec
import akka.testkit.EventFilter
import akka.testkit.TestKit
import akka.testkit.TestProbe

class CoordinatedShutdownSpec
    extends AkkaSpec(ConfigFactory.parseString("""
    akka.loglevel=INFO
    akka.loggers = ["akka.testkit.TestEventListener"]
  """)) {

  def extSys = system.asInstanceOf[ExtendedActorSystem]

  // some convenience to make the test readable
  def phase(dependsOn: String*): Phase = Phase(dependsOn.toSet, timeout = 10.seconds, recover = true, enabled = true)
  val emptyPhase: Phase = Phase(Set.empty, timeout = 10.seconds, recover = true, enabled = true)

  private def checkTopologicalSort(phases: Map[String, Phase]): List[String] = {
    val result = CoordinatedShutdown.topologicalSort(phases)
    result.zipWithIndex.foreach {
      case (phase, i) =>
        phases.get(phase) match {
          case Some(Phase(dependsOn, _, _, _)) =>
            dependsOn.foreach { depPhase =>
              withClue(
                s"phase [$phase] depends on [$depPhase] but was ordered before it in topological sort result $result") {
                i should be > result.indexOf(depPhase)
              }
            }
          case None => // ok
        }
    }
    result
  }

  case object CustomReason extends CoordinatedShutdown.Reason

  "CoordinatedShutdown" must {

    "sort phases in topological order" in {
      checkTopologicalSort(Map.empty) should ===(Nil)

      checkTopologicalSort(Map("a" -> emptyPhase)) should ===(List("a"))

      checkTopologicalSort(Map("b" -> phase("a"))) should ===(List("a", "b"))

      val result1 = checkTopologicalSort(Map("c" -> phase("a"), "b" -> phase("a")))
      result1.head should ===("a")
      // b, c can be in any order
      result1.toSet should ===(Set("a", "b", "c"))

      checkTopologicalSort(Map("b" -> phase("a"), "c" -> phase("b"))) should ===(List("a", "b", "c"))

      checkTopologicalSort(Map("b" -> phase("a"), "c" -> phase("a", "b"))) should ===(List("a", "b", "c"))

      val result2 = checkTopologicalSort(Map("c" -> phase("a", "b")))
      result2.last should ===("c")
      // a, b can be in any order
      result2.toSet should ===(Set("a", "b", "c"))

      checkTopologicalSort(Map("b" -> phase("a"), "c" -> phase("b"), "d" -> phase("b", "c"), "e" -> phase("d"))) should ===(
        List("a", "b", "c", "d", "e"))

      val result3 =
        checkTopologicalSort(Map("a2" -> phase("a1"), "a3" -> phase("a2"), "b2" -> phase("b1"), "b3" -> phase("b2")))
      val (a, b) = result3.partition(_.charAt(0) == 'a')
      a should ===(List("a1", "a2", "a3"))
      b should ===(List("b1", "b2", "b3"))
    }

    "detect cycles in phases (non-DAG)" in {
      intercept[IllegalArgumentException] {
        CoordinatedShutdown.topologicalSort(Map("a" -> phase("a")))
      }

      intercept[IllegalArgumentException] {
        CoordinatedShutdown.topologicalSort(Map("b" -> phase("a"), "a" -> phase("b")))
      }

      intercept[IllegalArgumentException] {
        CoordinatedShutdown.topologicalSort(Map("c" -> phase("a"), "c" -> phase("b"), "b" -> phase("c")))
      }

      intercept[IllegalArgumentException] {
        CoordinatedShutdown.topologicalSort(
          Map("d" -> phase("a"), "d" -> phase("c"), "c" -> phase("b"), "b" -> phase("d")))
      }

    }

    "have pre-defined phases from config" in {
      import CoordinatedShutdown._
      CoordinatedShutdown(system).orderedPhases should ===(
        List(
          PhaseBeforeServiceUnbind,
          PhaseServiceUnbind,
          PhaseServiceRequestsDone,
          PhaseServiceStop,
          PhaseBeforeClusterShutdown,
          PhaseClusterShardingShutdownRegion,
          PhaseClusterLeave,
          PhaseClusterExiting,
          PhaseClusterExitingDone,
          PhaseClusterShutdown,
          PhaseBeforeActorSystemTerminate,
          PhaseActorSystemTerminate))
    }

    "run ordered phases" in {
      import system.dispatcher
      val phases = Map("a" -> emptyPhase, "b" -> phase("a"), "c" -> phase("b", "a"))
      val co = new CoordinatedShutdown(extSys, phases)
      co.addTask("a", "a1") { () =>
        testActor ! "A"
        Future.successful(Done)
      }
      co.addTask("b", "b1") { () =>
        testActor ! "B"
        Future.successful(Done)
      }
      co.addTask("b", "b2") { () =>
        Future {
          // to verify that c is not performed before b
          Thread.sleep(100)
          testActor ! "B"
          Done
        }
      }
      co.addTask("c", "c1") { () =>
        testActor ! "C"
        Future.successful(Done)
      }
      whenReady(co.run(UnknownReason)) { _ =>
        receiveN(4) should ===(List("A", "B", "B", "C"))
      }
    }

    "cancel shutdown tasks" in {
      import system.dispatcher
      val phases = Map("a" -> emptyPhase)
      val co = new CoordinatedShutdown(extSys, phases)
      val probe = TestProbe()
      def createTask(message: String): () => Future[Done] =
        () =>
          Future {
            probe.ref ! message
            Done
          }

      val task1 = co.addCancellableTask("a", "copy1")(createTask("copy1"))
      val task2 = co.addCancellableTask("a", "copy2")(createTask("copy2"))
      val task3 = co.addCancellableTask("a", "copy3")(createTask("copy3"))

      assert(!task1.isCancelled)
      assert(!task2.isCancelled)
      assert(!task3.isCancelled)

      task2.cancel()
      assert(task2.isCancelled)

      val messagesFut = Future {
        probe.receiveN(2, 3.seconds).map(_.toString)
      }
      whenReady(co.run(UnknownReason).flatMap(_ => messagesFut), timeout(250.milliseconds)) { messages =>
        messages.distinct.size shouldEqual 2
        messages.foreach {
          case "copy1" | "copy3" => // OK
          case other             => fail(s"Unexpected probe message ${other}!")
        }
      }
    }

    "re-register the same task if requested" in {
      import system.dispatcher
      val phases = Map("a" -> emptyPhase)
      val co = new CoordinatedShutdown(extSys, phases)
      val testProbe = TestProbe()

      val taskName = "labor"
      val task: () => Future[Done] = () =>
        Future {
          testProbe.ref ! taskName
          Done
        }

      val task1 = co.addCancellableTask("a", taskName)(task)
      val task2 = co.addCancellableTask("a", taskName)(task)
      val task3 = co.addCancellableTask("a", taskName)(task)

      List(task1, task2, task3).foreach { t =>
        assert(!t.isCancelled)
      }

      task1.cancel()
      assert(task1.isCancelled)

      val messagesFut = Future {
        testProbe.receiveN(2, 3.seconds).map(_.toString)
      }
      whenReady(co.run(UnknownReason).flatMap(_ => messagesFut), timeout(250.milliseconds)) { messages =>
        messages.distinct.size shouldEqual 1
        messages.head shouldEqual taskName
      }
    }

    "honor registration and cancellation in later phases" in {
      import system.dispatcher
      val phases = Map("a" -> emptyPhase, "b" -> phase("a"))
      val co = new CoordinatedShutdown(extSys, phases)

      val testProbe = TestProbe()

      object TaskAB {
        val taskA: Cancellable = co.addCancellableTask("a", "taskA") { () =>
          Future {
            taskB.cancel()
            testProbe.ref ! "A cancels B"
            Done
          }
        }

        val taskB: Cancellable = co.addCancellableTask("b", "taskB") { () =>
          Future {
            taskA.cancel()
            testProbe.ref ! "B cancels A"
            Done
          }
        }
      }
      co.addCancellableTask("a", "taskA") { () =>
        Future {
          co.addCancellableTask("b", "dependentTaskB") { () =>
            Future {
              testProbe.ref ! "A adds B"
              Done
            }
          }
          Done
        }
      }
      co.addCancellableTask("a", "taskA") { () =>
        Future {
          co.addCancellableTask("a", "dependentTaskA") { () =>
            Future {
              testProbe.ref ! "A adds A"
              Done
            }
          }
          Done
        }
      }
      co.addCancellableTask("b", "taskB") { () =>
        Future {
          co.addCancellableTask("a", "dependentTaskA") { () =>
            Future {
              testProbe.ref ! "B adds A"
              Done
            }
          }
          Done
        }
      }

      List(TaskAB.taskA, TaskAB.taskB).foreach { t =>
        t.isCancelled shouldBe false
      }

      val messagesFut = Future {
        testProbe.receiveN(2, 3.seconds).map(_.toString)
      }
      whenReady(co.run(UnknownReason).flatMap(_ => messagesFut), timeout(250.milliseconds)) { messages =>
        messages.toSet shouldEqual Set("A adds B", "A cancels B")
      }
    }

    "cancel tasks across threads" in {
      val phases = Map("a" -> emptyPhase, "b" -> phase("a"))
      val co = new CoordinatedShutdown(extSys, phases)
      val testProbe = TestProbe()

      val executor = Executors.newFixedThreadPool(25)
      implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(executor)

      case class BMessage(content: String)

      val messageA = "concurrentA"
      val task: () => Future[Done] = () =>
        Future {
          testProbe.ref ! messageA
          co.addCancellableTask("b", "concurrentB") { () =>
            Future {
              testProbe.ref ! BMessage("concurrentB")
              Done
            }(ExecutionContext.parasitic)
          }
          Done
        }(ExecutionContext.parasitic)

      val cancellationFut: Future[Done] = {
        val cancellables = (0 until 20).map { _ =>
          co.addCancellableTask("a", "concurrentTaskA")(task)
        }
        val shouldBeCancelled = cancellables.zipWithIndex.collect {
          case (c, i) if i % 2 == 0 => c
        }
        val cancelFutures = for {
          _ <- cancellables
          c <- shouldBeCancelled
        } yield Future {
          c.cancel() shouldBe true
          Done
        }
        cancelFutures.foldLeft(Future.successful(Done)) {
          case (acc, fut) =>
            acc.flatMap(_ => fut)
        }
      }

      Await.result(cancellationFut, 250.milliseconds)

      val messagesFut = Future {
        testProbe.receiveN(20, 3.seconds).map(_.toString)
      }
      whenReady(co.run(UnknownReason).flatMap(_ => messagesFut), timeout(250.milliseconds)) { messages =>
        messages.length shouldEqual 20
        messages.toSet shouldEqual Set(messageA, "BMessage(concurrentB)")
      }

      executor.shutdown()
    }

    "run from a given phase" in {
      val phases = Map("a" -> emptyPhase, "b" -> phase("a"), "c" -> phase("b", "a"))
      val co = new CoordinatedShutdown(extSys, phases)
      co.addTask("a", "a1") { () =>
        testActor ! "A"
        Future.successful(Done)
      }
      co.addTask("b", "b1") { () =>
        testActor ! "B"
        Future.successful(Done)
      }
      co.addTask("c", "c1") { () =>
        testActor ! "C"
        Future.successful(Done)
      }
      Await.result(co.run(CustomReason, Some("b")), remainingOrDefault)
      receiveN(2) should ===(List("B", "C"))
      co.shutdownReason() should ===(Some(CustomReason))
    }

    "only run once" in {
      val phases = Map("a" -> emptyPhase)
      val co = new CoordinatedShutdown(extSys, phases)
      co.addTask("a", "a1") { () =>
        testActor ! "A"
        Future.successful(Done)
      }
      co.shutdownReason() should ===(None)
      Await.result(co.run(CustomReason), remainingOrDefault)
      co.shutdownReason() should ===(Some(CustomReason))
      expectMsg("A")
      Await.result(co.run(UnknownReason), remainingOrDefault)
      testActor ! "done"
      expectMsg("done") // no additional A
      co.shutdownReason() should ===(Some(CustomReason))
    }

    "continue after timeout or failure" in {
      import system.dispatcher
      val phases = Map(
        "a" -> emptyPhase,
        "b" -> Phase(dependsOn = Set("a"), timeout = 100.millis, recover = true, enabled = true),
        "c" -> phase("b", "a"))
      val co = new CoordinatedShutdown(extSys, phases)
      co.addTask("a", "a1") { () =>
        testActor ! "A"
        Future.failed(new RuntimeException("boom"))
      }
      co.addTask("a", "a2") { () =>
        Future {
          // to verify that b is not performed before a also in case of failure
          Thread.sleep(100)
          testActor ! "A"
          Done
        }
      }
      co.addTask("b", "b1") { () =>
        testActor ! "B"
        Promise[Done]().future // never completed
      }
      co.addTask("c", "c1") { () =>
        testActor ! "C"
        Future.successful(Done)
      }
      EventFilter.warning(message = "Task [a1] failed in phase [a]: boom", occurrences = 1).intercept {
        EventFilter
          .warning(message = "Coordinated shutdown phase [b] timed out after 100 milliseconds", occurrences = 1)
          .intercept {
            Await.result(co.run(UnknownReason), remainingOrDefault)
          }
      }
      expectMsg("A")
      expectMsg("A")
      expectMsg("B")
      expectMsg("C")
    }

    "abort if recover=off" in {
      val phases = Map(
        "a" -> emptyPhase,
        "b" -> Phase(dependsOn = Set("a"), timeout = 100.millis, recover = false, enabled = true),
        "c" -> phase("b", "a"))
      val co = new CoordinatedShutdown(extSys, phases)
      co.addTask("b", "b1") { () =>
        testActor ! "B"
        Promise[Done]().future // never completed
      }
      co.addTask("c", "c1") { () =>
        testActor ! "C"
        Future.successful(Done)
      }
      val result = co.run(UnknownReason)
      expectMsg("B")
      intercept[TimeoutException] {
        Await.result(result, remainingOrDefault)
      }
      expectNoMessage() // C not run
    }

    "skip tasks in disabled phase" in {
      val phases = Map(
        "a" -> emptyPhase,
        "b" -> Phase(dependsOn = Set("a"), timeout = 100.millis, recover = false, enabled = false),
        "c" -> phase("b", "a"))
      val co = new CoordinatedShutdown(extSys, phases)
      co.addTask("b", "b1") { () =>
        testActor ! "B"
        Future.failed(new RuntimeException("Was expected to not be executed"))
      }
      co.addTask("c", "c1") { () =>
        testActor ! "C"
        Future.successful(Done)
      }
      EventFilter.info(start = "Phase [b] disabled through configuration", occurrences = 1).intercept {
        val result = co.run(UnknownReason)
        expectMsg("C")
        result.futureValue should ===(Done)
      }
    }

    "be possible to add tasks in later phase from task in earlier phase" in {
      val phases = Map("a" -> emptyPhase, "b" -> phase("a"))
      val co = new CoordinatedShutdown(extSys, phases)
      co.addTask("a", "a1") { () =>
        testActor ! "A"
        co.addTask("b", "b1") { () =>
          testActor ! "B"
          Future.successful(Done)
        }
        Future.successful(Done)
      }
      Await.result(co.run(UnknownReason), remainingOrDefault)
      expectMsg("A")
      expectMsg("B")
    }

    "parse phases from config" in {
      CoordinatedShutdown.phasesFromConfig(ConfigFactory.parseString("""
        default-phase-timeout = 10s
        phases {
          a = {}
          b {
            depends-on = [a]
            timeout = 15s
          }
          c {
            depends-on = [a, b]
            recover = off
          }
        }
        """)) should ===(
        Map(
          "a" -> Phase(dependsOn = Set.empty, timeout = 10.seconds, recover = true, enabled = true),
          "b" -> Phase(dependsOn = Set("a"), timeout = 15.seconds, recover = true, enabled = true),
          "c" -> Phase(dependsOn = Set("a", "b"), timeout = 10.seconds, recover = false, enabled = true)))
    }

    "default exit code to 0" in {
      lazy val conf = ConfigFactory.load().getConfig("akka.coordinated-shutdown")
      val confWithOverrides = CoordinatedShutdown.confWithOverrides(conf, None)
      confWithOverrides.getInt("exit-code") should ===(0)
    }

    "default exit code to -1 when the Reason is ClusterDowning" in {
      lazy val conf = ConfigFactory.load().getConfig("akka.coordinated-shutdown")
      val confWithOverrides =
        CoordinatedShutdown.confWithOverrides(conf, Some(CoordinatedShutdown.ClusterDowningReason))
      confWithOverrides.getInt("exit-code") should ===(-1)
    }

    "terminate ActorSystem" in {
      val sys = ActorSystem(system.name, system.settings.config)
      try {
        Await.result(CoordinatedShutdown(sys).run(CustomReason), 10.seconds) should ===(Done)
        sys.whenTerminated.isCompleted should ===(true)
        CoordinatedShutdown(sys).shutdownReason() should ===(Some(CustomReason))
      } finally {
        shutdown(sys)
      }
    }

    "be run by ActorSystem.terminate" in {
      val sys = ActorSystem(system.name, system.settings.config)
      try {
        Await.result(sys.terminate(), 10.seconds)
        sys.whenTerminated.isCompleted should ===(true)
        CoordinatedShutdown(sys).shutdownReason() should ===(Some(CoordinatedShutdown.ActorSystemTerminateReason))
      } finally {
        shutdown(sys)
      }
    }

    "not be run by ActorSystem.terminate when run-by-actor-system-terminate=off" in {
      val sys = ActorSystem(
        system.name,
        ConfigFactory
          .parseString("akka.coordinated-shutdown.run-by-actor-system-terminate = off")
          .withFallback(system.settings.config))
      try {
        Await.result(sys.terminate(), 10.seconds)
        sys.whenTerminated.isCompleted should ===(true)
        CoordinatedShutdown(sys).shutdownReason() should ===(None)
      } finally {
        shutdown(sys)
      }
    }

    "not allow terminate-actor-system=off && run-by-actor-system-terminate=on" in {
      intercept[ConfigurationException] {
        val sys = ActorSystem(
          system.name,
          ConfigFactory
            .parseString("akka.coordinated-shutdown.terminate-actor-system = off")
            .withFallback(system.settings.config))
        // will only get here if test is failing
        shutdown(sys)
      }
    }

    "add and remove user JVM hooks with run-by-jvm-shutdown-hook = off, terminate-actor-system = off" in new JvmHookTest {
      lazy val systemName = s"CoordinatedShutdownSpec-JvmHooks-1-${System.currentTimeMillis()}"
      lazy val systemConfig = ConfigFactory.parseString("""
          akka.coordinated-shutdown.run-by-jvm-shutdown-hook = off
          akka.coordinated-shutdown.terminate-actor-system = off
          akka.coordinated-shutdown.run-by-actor-system-terminate = off
        """)

      override def withSystemRunning(newSystem: ActorSystem, coordinatedShutdown: CoordinatedShutdown): Unit = {
        val cancellable =
          coordinatedShutdown.addCancellableJvmShutdownHook(println(s"User JVM hook from ${newSystem.name}"))
        myHooksCount should ===(1) // one user, none from system
        cancellable.cancel()
      }
    }

    "add and remove user JVM hooks with run-by-jvm-shutdown-hook = on, terminate-actor-system = off" in new JvmHookTest {
      lazy val systemName = s"CoordinatedShutdownSpec-JvmHooks-2-${System.currentTimeMillis()}"
      lazy val systemConfig = ConfigFactory.parseString("""
          akka.coordinated-shutdown.run-by-jvm-shutdown-hook = on
          akka.coordinated-shutdown.terminate-actor-system = off
          akka.coordinated-shutdown.run-by-actor-system-terminate = off
        """)

      override def withSystemRunning(newSystem: ActorSystem, coordinatedShutdown: CoordinatedShutdown): Unit = {
        val cancellable =
          coordinatedShutdown.addCancellableJvmShutdownHook(println(s"User JVM hook from ${newSystem.name}"))
        myHooksCount should ===(2) // one user, one from system

        cancellable.cancel()
      }
    }

    "add and remove user JVM hooks with run-by-jvm-shutdown-hook = on, terminate-actor-system = on" in new JvmHookTest {
      lazy val systemName = s"CoordinatedShutdownSpec-JvmHooks-3-${System.currentTimeMillis()}"
      lazy val systemConfig = ConfigFactory.parseString("""
          akka.coordinated-shutdown.run-by-jvm-shutdown-hook = on
          akka.coordinated-shutdown.terminate-actor-system = on
        """)

      def withSystemRunning(newSystem: ActorSystem, coordinatedShutdown: CoordinatedShutdown): Unit = {
        val cancellable =
          coordinatedShutdown.addCancellableJvmShutdownHook(println(s"User JVM hook from ${newSystem.name}"))
        myHooksCount should ===(2) // one user, one from actor system
        cancellable.cancel()
      }
    }

    "add and remove user JVM hooks with run-by-jvm-shutdown-hook = on, akka.jvm-shutdown-hooks = off" in new JvmHookTest {
      lazy val systemName = s"CoordinatedShutdownSpec-JvmHooks-4-${System.currentTimeMillis()}"
      lazy val systemConfig = ConfigFactory.parseString("""
          akka.jvm-shutdown-hooks = off
          akka.coordinated-shutdown.run-by-jvm-shutdown-hook = on
        """)

      def withSystemRunning(newSystem: ActorSystem, coordinatedShutdown: CoordinatedShutdown): Unit = {
        val cancellable =
          coordinatedShutdown.addCancellableJvmShutdownHook(println(s"User JVM hook from ${newSystem.name}"))
        myHooksCount should ===(1) // one user, none from actor system
        cancellable.cancel()
      }
    }

    "access extension after system termination" in new JvmHookTest {
      lazy val systemName = s"CoordinatedShutdownSpec-terminated-${System.currentTimeMillis()}"
      lazy val systemConfig = ConfigFactory.parseString("""
          akka.coordinated-shutdown.run-by-jvm-shutdown-hook = on
          akka.coordinated-shutdown.terminate-actor-system = on
        """)

      def withSystemRunning(newSystem: ActorSystem, coordinatedShutdown: CoordinatedShutdown): Unit = {
        TestKit.shutdownActorSystem(newSystem)
        CoordinatedShutdown(newSystem)

      }
    }

    def withCoordinatedShutdown(block: (ActorSystem, CoordinatedShutdown) => Unit): Unit = {
      val system = ActorSystem(
        s"CoordinatedShutdownSpec-terminated-${System.currentTimeMillis()}",
        ConfigFactory.parseString("""
          akka.coordinated-shutdown.phases {
            before-actor-system-terminate {

            }
            a {
              # as late as possible
              dependsOn = [before-actor-system-terminate]
              timeout=10s
              recover=off
              enabled=on
            }

            b {
              dependsOn = [a]
              timeout=10s
              recover=off
              enabled=on
            }
          }
          """))
      try {
        block(system, CoordinatedShutdown(system))
      } finally {
        TestKit.shutdownActorSystem(system)
      }
    }

    "support actor termination tasks with a stop message" in {
      withCoordinatedShutdown { (system, co) =>
        val actorToWatch = TestProbe()(system)
        co.addActorTerminationTask("before-actor-system-terminate", "a1", actorToWatch.ref, Some("stop"))
        val result = co.run(UnknownReason)
        actorToWatch.expectMsg("stop")
        result.isReadyWithin(100.millis) should be(false)
        actorToWatch.ref ! PoisonPill
        result.futureValue should ===(Done)
      }
    }

    "support actor termination tasks without a stop message" in {
      withCoordinatedShutdown { (system, co) =>
        val actorToWatch = TestProbe()(system)
        co.addActorTerminationTask("before-actor-system-terminate", "a1", actorToWatch.ref, None)
        val result = co.run(UnknownReason)
        actorToWatch.expectNoMessage(100.millis)
        result.isReadyWithin(100.millis) should be(false)
        actorToWatch.ref ! PoisonPill
        result.futureValue should ===(Done)
      }
    }

    "support actor termination tasks for actors that are already shutdown" in {
      withCoordinatedShutdown { (system, co) =>
        val actorToWatch = TestProbe()(system)
        watch(actorToWatch.ref)
        actorToWatch.ref ! PoisonPill
        expectTerminated(actorToWatch.ref)
        co.addActorTerminationTask("before-actor-system-terminate", "a1", actorToWatch.ref, None)
        val result = co.run(UnknownReason)
        result.futureValue should ===(Done)
      }
    }

    "allow watching the same actor twice in the same phase" in {
      withCoordinatedShutdown { (system, co) =>
        val actorToWatch = TestProbe()(system)
        co.addActorTerminationTask("before-actor-system-terminate", "a1", actorToWatch.ref, Some("stop1"))
        co.addActorTerminationTask("before-actor-system-terminate", "a2", actorToWatch.ref, Some("stop2"))
        val result = co.run(UnknownReason)
        actorToWatch.expectMsgAllOf("stop1", "stop2")
        actorToWatch.ref ! PoisonPill
        result.futureValue should ===(Done)
      }
    }

    "allow watching the same actor twice in different phases" in {
      withCoordinatedShutdown { (system, co) =>
        val actorToWatch = TestProbe()(system)
        // arbitrary phase that runs before the phase of b1
        co.addActorTerminationTask("service-stop", "a1", actorToWatch.ref, Some("stopa"))
        // no stop message because it's just going to end up being dead lettered
        co.addActorTerminationTask("before-actor-system-terminate", "b1", actorToWatch.ref, None)
        val result = co.run(UnknownReason)
        actorToWatch.expectMsg("stopa")
        actorToWatch.expectNoMessage(100.millis)
        actorToWatch.ref ! PoisonPill
        result.futureValue should ===(Done)
      }
    }

  }

  abstract class JvmHookTest {

    def systemName: String
    def systemConfig: Config
    def withSystemRunning(system: ActorSystem, cs: CoordinatedShutdown): Unit

    private val newSystem =
      ActorSystem(systemName, systemConfig.withFallback(system.settings.config)).asInstanceOf[ExtendedActorSystem]
    private var shutdownHooks = Set.empty[Thread]
    private val mockRuntime = new JVMShutdownHooks {
      override def addHook(t: Thread): Unit = synchronized {
        // mimic validation in JDK ApplicationShutdownHooks
        if (shutdownHooks == null)
          throw new IllegalStateException("Shutdown in progress");

        if (t.isAlive())
          throw new IllegalArgumentException("Hook already running");

        if (shutdownHooks.contains(t))
          throw new IllegalArgumentException("Hook previously registered");

        shutdownHooks += t
      }

      override def removeHook(t: Thread): Boolean = synchronized {
        // mimic validation in JDK ApplicationShutdownHooks
        if (t == null)
          throw new NullPointerException();

        if (shutdownHooks.contains(t)) {
          shutdownHooks -= t
          true
        } else false
      }
    }
    private val csConfig = newSystem.settings.config.getConfig("akka.coordinated-shutdown")
    // pretend extension creation and start
    private val cs = new CoordinatedShutdown(newSystem, CoordinatedShutdown.phasesFromConfig(csConfig), mockRuntime)
    CoordinatedShutdown.init(newSystem, csConfig, cs)

    withSystemRunning(newSystem, cs)

    TestKit.shutdownActorSystem(newSystem)
    shutdownHooks should have size (0)

    protected def myHooksCount: Int = synchronized(shutdownHooks.size)
  }

}
