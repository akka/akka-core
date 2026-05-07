/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.util.Random

import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.cluster.sharding.ShardCoordinator.PendingGetShardHomes
import akka.testkit.TestKit
import akka.testkit.TestProbe
import akka.actor.ActorSystem

class PendingGetShardHomesSpec extends AnyWordSpec with BeforeAndAfterAll with Matchers {
  val Empty = new PendingGetShardHomes(Map.empty)
  var testKit: TestKit = _
  implicit def system: ActorSystem = testKit.system

  override def beforeAll(): Unit = {
    testKit = new TestKit(ActorSystem("PendingGetShardHomesSpec"))
  }

  def randomShard(): ShardRegion.ShardId = Random.nextInt(1000).toString

  "PendingGetShardHomes" should {
    "be able to add requests with no/ignored replyTo" in {
      val shard = randomShard()
      val postAdd = Empty.addRequest(shard, None)

      postAdd.requestsByShard.keySet should contain(shard)
      postAdd.requestsByShard(shard) shouldBe empty
    }

    "be able to add requests with a replyTo" in {
      val shard = randomShard()
      val replyToProbe = TestProbe()
      val postAdd = Empty.addRequest(shard, Some(replyToProbe.ref))

      postAdd.requestsByShard.keySet should contain(shard)
      postAdd.requestsByShard(shard) should contain(replyToProbe.ref)
    }

    "be able to add multiple requests for a shard" in {
      val shard = randomShard()
      val replyToProbe = TestProbe()
      val postAdds =
        Empty.addRequest(shard, None).addRequest(shard, Some(replyToProbe.ref))

      postAdds.requestsByShard.keySet should contain(shard)
      postAdds.requestsByShard(shard) should contain(replyToProbe.ref)
    }

    "be able to remove requests for a shard" in {
      val shard1 = randomShard()
      val replyToProbe1 = TestProbe()
      val replyToProbe2 = TestProbe()
      val postAdds1 =
        Empty.addRequest(shard1, Some(replyToProbe1.ref)).addRequest(shard1, Some(replyToProbe2.ref))

      val shard2 = randomShard()
      val postAdds2 = postAdds1.addRequest(shard2, None)

      val (r2s, postRemove) = postAdds2.removeRequestsForShard(shard1)
      (r2s should contain).allOf(replyToProbe1.ref, replyToProbe2.ref)
      // if shard1 == shard2, there are no requests by shard, otherwise shard2 remains
      postRemove.requestsByShard.keySet should contain theSameElementsAs Set(shard1, shard2).excl(shard1)
      if (shard1 != shard2) {
        postRemove.requestsByShard(shard2) shouldBe empty
      }
    }

    "should remove a request for the shard with the most requests" in {
      val shard1 = randomShard()
      val replyToProbe1 = TestProbe()
      val replyToProbe2 = TestProbe()
      val shard1r2s = Set(replyToProbe1.ref, replyToProbe2.ref)
      val postAdds1 =
        Empty.addRequest(shard1, Some(replyToProbe1.ref)).addRequest(shard1, Some(replyToProbe2.ref))

      val shard2 = {
        var s = shard1
        while (s == shard1) {
          s = randomShard()
        }
        s
      }
      val postAdds2 = postAdds1.addRequest(shard2, None)

      val (reqOpt1, postRemove1) = postAdds2.removeRequest()
      reqOpt1 shouldNot be(empty)
      reqOpt1.foreach { req =>
        req._1 shouldBe shard1
        shard1r2s should contain(req._2.get)
        postRemove1.requestsByShard.keySet should contain theSameElementsAs (postAdds2.requestsByShard.keySet)
        postRemove1.requestsByShard(shard1) should contain theSameElementsAs (shard1r2s -- req._2.toSet)
        postRemove1.requestsByShard(shard2) shouldBe postAdds2.requestsByShard(shard2)
      }

      val (reqOpt2, postRemove2) = postRemove1.removeRequest()
      reqOpt2 shouldNot be(empty)
      reqOpt2.foreach { req =>
        req._1 shouldBe shard1 // shard1 size is 1, shard2 is empty (only ignoreRef)
        postRemove1.requestsByShard(shard1) should contain(req._2.get)
        postRemove2.requestsByShard.keySet should contain only (shard2)
        postRemove2.requestsByShard(shard2) shouldBe postAdds2.requestsByShard(shard2)
      }

      val (reqOpt3, postRemove3) = postRemove2.removeRequest()
      reqOpt3 shouldNot be(empty)
      reqOpt3.foreach { req =>
        req._1 shouldBe shard2
        req._2 shouldBe empty
        postRemove3.requestsByShard shouldBe empty
      }

      val (reqOpt4, postRemove4) = postRemove3.removeRequest()
      reqOpt4 shouldBe empty
      postRemove4 shouldBe postRemove3
    }
  }

  override def afterAll(): Unit = {
    testKit.shutdown()
  }
}
