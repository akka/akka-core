/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch.sysmsg

import akka.actor.Props
import akka.testkit.AkkaSpec

class SystemMessageListSpec extends AkkaSpec {
  import SystemMessageList.ENil
  import SystemMessageList.LNil

  val child = system.actorOf(Props.empty, "dummy") // need an ActorRef for the Failed msg

  "The SystemMessageList value class" must {

    "handle empty lists correctly" in {
      LNil.head should ===(null)
      LNil.isEmpty should ===(true)
      (LNil.reverse == ENil) should ===(true)
    }

    "able to append messages" in {
      val create0 = Failed(child, null, 0)
      val create1 = Failed(child, null, 1)
      val create2 = Failed(child, null, 2)
      ((create0 :: LNil).head eq create0) should ===(true)
      ((create1 :: create0 :: LNil).head eq create1) should ===(true)
      ((create2 :: create1 :: create0 :: LNil).head eq create2) should ===(true)

      (create2.next eq create1) should ===(true)
      (create1.next eq create0) should ===(true)
      (create0.next eq null) should ===(true)
    }

    "able to deconstruct head and tail" in {
      val create0 = Failed(child, null, 0)
      val create1 = Failed(child, null, 1)
      val create2 = Failed(child, null, 2)
      val list = create2 :: create1 :: create0 :: LNil

      (list.head eq create2) should ===(true)
      (list.tail.head eq create1) should ===(true)
      (list.tail.tail.head eq create0) should ===(true)
      (list.tail.tail.tail.head eq null) should ===(true)
    }

    "properly report size and emptyness" in {
      val create0 = Failed(child, null, 0)
      val create1 = Failed(child, null, 1)
      val create2 = Failed(child, null, 2)
      val list = create2 :: create1 :: create0 :: LNil

      list.size should ===(3)
      list.isEmpty should ===(false)

      list.tail.size should ===(2)
      list.tail.isEmpty should ===(false)

      list.tail.tail.size should ===(1)
      list.tail.tail.isEmpty should ===(false)

      list.tail.tail.tail.size should ===(0)
      list.tail.tail.tail.isEmpty should ===(true)

    }

    "properly reverse contents" in {
      val create0 = Failed(child, null, 0)
      val create1 = Failed(child, null, 1)
      val create2 = Failed(child, null, 2)
      val list = create2 :: create1 :: create0 :: LNil
      val listRev: EarliestFirstSystemMessageList = list.reverse

      listRev.isEmpty should ===(false)
      listRev.size should ===(3)

      (listRev.head eq create0) should ===(true)
      (listRev.tail.head eq create1) should ===(true)
      (listRev.tail.tail.head eq create2) should ===(true)
      (listRev.tail.tail.tail.head eq null) should ===(true)

      (create0.next eq create1) should ===(true)
      (create1.next eq create2) should ===(true)
      (create2.next eq null) should ===(true)
    }

  }

  "EarliestFirstSystemMessageList" must {

    "properly prepend reversed message lists to the front" in {
      val create0 = Failed(child, null, 0)
      val create1 = Failed(child, null, 1)
      val create2 = Failed(child, null, 2)
      val create3 = Failed(child, null, 3)
      val create4 = Failed(child, null, 4)
      val create5 = Failed(child, null, 5)

      val fwdList = create3 :: create4 :: create5 :: ENil
      val revList = create2 :: create1 :: create0 :: LNil

      val list = fwdList.reversePrepend(revList)

      (list.head eq create0) should ===(true)
      (list.tail.head eq create1) should ===(true)
      (list.tail.tail.head eq create2) should ===(true)
      (list.tail.tail.tail.head eq create3) should ===(true)
      (list.tail.tail.tail.tail.head eq create4) should ===(true)
      (list.tail.tail.tail.tail.tail.head eq create5) should ===(true)
      (list.tail.tail.tail.tail.tail.tail.head eq null) should ===(true)

      ENil.reversePrepend(LNil) == ENil should ===(true)
      (ENil.reversePrepend(create0 :: LNil).head eq create0) should ===(true)
      ((create0 :: ENil).reversePrepend(LNil).head eq create0) should ===(true)
    }

  }
}
