/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.cluster.sharding.typed

import akka.Done

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.pattern.StatusReply
import akka.persistence.typed.PersistenceId
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object AccountExampleSpec {
  val config = ConfigFactory.parseString("""
      akka.actor.provider = cluster

      akka.remote.artery.canonical.port = 0
      akka.remote.artery.canonical.hostname = 127.0.0.1
      
      akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
      akka.persistence.journal.inmem.test-serialization = on
      """)

}

class AccountExampleSpec
    extends ScalaTestWithActorTestKit(AccountExampleSpec.config)
    with AnyWordSpecLike
    with LogCapturing {
  import AccountExampleWithEventHandlersInState.AccountEntity
  import AccountExampleWithEventHandlersInState.AccountEntity._

  private val sharding = ClusterSharding(system)

  override def beforeAll(): Unit = {
    super.beforeAll()
    Cluster(system).manager ! Join(Cluster(system).selfMember.address)

    sharding.init(Entity(AccountEntity.TypeKey) { entityContext =>
      AccountEntity(entityContext.entityId, PersistenceId(entityContext.entityTypeKey.name, entityContext.entityId))
    })
  }

  "Account example" must {

    "handle Deposit" in {
      val probe = createTestProbe[StatusReply[Done]]()
      val ref = ClusterSharding(system).entityRefFor(AccountEntity.TypeKey, "1")
      ref ! CreateAccount(probe.ref)
      probe.expectMessage(StatusReply.Ack)
      ref ! Deposit(100, probe.ref)
      probe.expectMessage(StatusReply.Ack)
      ref ! Deposit(10, probe.ref)
      probe.expectMessage(StatusReply.Ack)
    }

    "handle Withdraw" in {
      val doneProbe = createTestProbe[StatusReply[Done]]()
      val ref = ClusterSharding(system).entityRefFor(AccountEntity.TypeKey, "2")
      ref ! CreateAccount(doneProbe.ref)
      doneProbe.expectMessage(StatusReply.Ack)
      ref ! Deposit(100, doneProbe.ref)
      doneProbe.expectMessage(StatusReply.Ack)
      ref ! Withdraw(10, doneProbe.ref)
      doneProbe.expectMessage(StatusReply.Ack)

      val balanceProbe = createTestProbe[CurrentBalance]()
      ref ! GetBalance(balanceProbe.ref)
      balanceProbe.expectMessage(CurrentBalance(90))
    }

    "reject Withdraw overdraft" in {
      val probe = createTestProbe[StatusReply[Done]]()
      val ref = ClusterSharding(system).entityRefFor[Command](AccountEntity.TypeKey, "3")
      ref ! CreateAccount(probe.ref)
      probe.expectMessage(StatusReply.Ack)
      ref ! Deposit(100, probe.ref)
      probe.expectMessage(StatusReply.Ack)
      ref ! Withdraw(110, probe.ref)
      probe.expectMessageType[StatusReply[Done]].isError should ===(true)

      // Account.Command is the command type, but it should also be possible to narrow it
      // ... thus restricting the entity ref from being sent other commands, e.g.:
      // val ref2 = ClusterSharding(system).entityRefFor[Deposit](AccountEntity.TypeKey, "3")
      // val probe2 = createTestProbe[CurrentBalance]()
      // val msg = GetBalance(probe2.ref)
      // ref2 ! msg // type mismatch: GetBalance NOT =:= Deposit
    }

    "handle GetBalance" in {
      val opProbe = createTestProbe[StatusReply[Done]]()
      val ref = ClusterSharding(system).entityRefFor(AccountEntity.TypeKey, "4")
      ref ! CreateAccount(opProbe.ref)
      opProbe.expectMessage(StatusReply.Ack)
      ref ! Deposit(100, opProbe.ref)
      opProbe.expectMessage(StatusReply.Ack)

      val getProbe = createTestProbe[CurrentBalance]()
      ref ! GetBalance(getProbe.ref)
      getProbe.expectMessage(CurrentBalance(100))
    }

    "be usable with ask" in {
      val ref = ClusterSharding(system).entityRefFor(AccountEntity.TypeKey, "5")
      val createResult: Future[Done] = ref.askWithStatus(CreateAccount(_))
      createResult.futureValue should ===(Done)
      implicit val ec: ExecutionContext = testKit.system.executionContext

      // Errors are shown in IntelliJ Scala plugin 2019.1.6, but compiles with Scala 2.12.8.
      // Ok in IntelliJ if using ref.ask[OperationResult].
      val deposited = ref.askWithStatus(Deposit(100, _)).futureValue
      deposited should ===(Done)
      val withdrawn = ref.askWithStatus(Withdraw(10, _)).futureValue
      withdrawn should ===(Done)
      ref.ask(GetBalance.apply).map(_.balance).futureValue should ===(90)
    }

    "verifySerialization" in {
      val opProbe = createTestProbe[StatusReply[Done]]()
      serializationTestKit.verifySerialization(CreateAccount(opProbe.ref))
      serializationTestKit.verifySerialization(Deposit(100, opProbe.ref))
      serializationTestKit.verifySerialization(Withdraw(90, opProbe.ref))
      serializationTestKit.verifySerialization(CloseAccount(opProbe.ref))

      val getProbe = createTestProbe[CurrentBalance]()
      serializationTestKit.verifySerialization(GetBalance(getProbe.ref))

      serializationTestKit.verifySerialization(CurrentBalance(100))

      serializationTestKit.verifySerialization(AccountCreated)
      serializationTestKit.verifySerialization(Deposited(100))
      serializationTestKit.verifySerialization(Withdrawn(90))
      serializationTestKit.verifySerialization(AccountClosed)

      serializationTestKit.verifySerialization(EmptyAccount)
      serializationTestKit.verifySerialization(OpenedAccount(100))
      serializationTestKit.verifySerialization(ClosedAccount)
    }

  }
}
