/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.collection.immutable.SortedSet
import scala.util.Random

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.Address
import akka.actor.AddressFromURIString
import akka.util.Version

class MemberOrderingSpec extends AnyWordSpec with Matchers {
  import Member.addressOrdering
  import Member.ordering
  import MemberStatus._

  def m(address: Address, status: MemberStatus): Member = TestMember(address, status)

  "An Ordering[Member]" must {

    "order members by host:port" in {
      val members = SortedSet.empty[Member] +
        m(AddressFromURIString("akka://sys@darkstar:1112"), Up) +
        m(AddressFromURIString("akka://sys@darkstar:1113"), Joining) +
        m(AddressFromURIString("akka://sys@darkstar:1111"), Up)

      val seq = members.toSeq
      seq.size should ===(3)
      seq(0) should ===(m(AddressFromURIString("akka://sys@darkstar:1111"), Up))
      seq(1) should ===(m(AddressFromURIString("akka://sys@darkstar:1112"), Up))
      seq(2) should ===(m(AddressFromURIString("akka://sys@darkstar:1113"), Joining))
    }

    "be sorted by address correctly" in {
      import Member.ordering
      // sorting should be done on host and port, only
      val m1 = m(Address("akka", "sys1", "host1", 9000), Up)
      val m2 = m(Address("akka", "sys1", "host1", 10000), Up)
      val m3 = m(Address("cluster", "sys2", "host2", 8000), Up)
      val m4 = m(Address("cluster", "sys2", "host2", 9000), Up)
      val m5 = m(Address("cluster", "sys1", "host2", 10000), Up)

      val expected = IndexedSeq(m1, m2, m3, m4, m5)
      val shuffled = Random.shuffle(expected)
      shuffled.sorted should ===(expected)
      (SortedSet.empty[Member] ++ shuffled).toIndexedSeq should ===(expected)
    }

    "have stable equals and hashCode" in {
      val address = Address("akka", "sys1", "host1", 9000)
      val m1 = m(address, Joining)
      val m11 = Member(UniqueAddress(address, -3L), Set.empty, Version.Zero)
      val m2 = m1.copy(status = Up)
      val m22 = m11.copy(status = Up)
      val m3 = m(address.copy(port = Some(10000)), Up)

      m1 should ===(m2)
      m1.hashCode should ===(m2.hashCode)

      m3 should not be (m2)
      m3 should not be (m1)

      m11 should ===(m22)
      m11.hashCode should ===(m22.hashCode)

      // different uid
      m1 should not be (m11)
      m2 should not be (m22)
    }

    "have consistent ordering and equals" in {
      val address1 = Address("akka", "sys1", "host1", 9001)
      val address2 = address1.copy(port = Some(9002))

      val x = m(address1, Exiting)
      val y = m(address1, Removed)
      val z = m(address2, Up)
      Member.ordering.compare(x, y) should ===(0)
      Member.ordering.compare(x, z) should ===(Member.ordering.compare(y, z))

      // different uid
      val a = m(address1, Joining)
      val b = Member(UniqueAddress(address1, -3L), Set.empty, Version.Zero)
      Member.ordering.compare(a, b) should ===(1)
      Member.ordering.compare(b, a) should ===(-1)

    }

    "work with SortedSet" in {
      val address1 = Address("akka", "sys1", "host1", 9001)
      val address2 = address1.copy(port = Some(9002))
      val address3 = address1.copy(port = Some(9003))

      (SortedSet(m(address1, Joining)) - m(address1, Up)) should ===(SortedSet.empty[Member])
      (SortedSet(m(address1, Exiting)) - m(address1, Removed)) should ===(SortedSet.empty[Member])
      (SortedSet(m(address1, Up)) - m(address1, Exiting)) should ===(SortedSet.empty[Member])
      (SortedSet(m(address2, Up), m(address3, Joining), m(address1, Exiting)) - m(address1, Removed)) should ===(
        SortedSet(m(address2, Up), m(address3, Joining)))
    }

  }

  "An Ordering[Address]" must {

    "order addresses by port" in {
      val addresses = SortedSet.empty[Address] +
        AddressFromURIString("akka://sys@darkstar:1112") +
        AddressFromURIString("akka://sys@darkstar:1113") +
        AddressFromURIString("akka://sys@darkstar:1110") +
        AddressFromURIString("akka://sys@darkstar:1111")

      val seq = addresses.toSeq
      seq.size should ===(4)
      seq(0) should ===(AddressFromURIString("akka://sys@darkstar:1110"))
      seq(1) should ===(AddressFromURIString("akka://sys@darkstar:1111"))
      seq(2) should ===(AddressFromURIString("akka://sys@darkstar:1112"))
      seq(3) should ===(AddressFromURIString("akka://sys@darkstar:1113"))
    }

    "order addresses by hostname" in {
      val addresses = SortedSet.empty[Address] +
        AddressFromURIString("akka://sys@darkstar2:1110") +
        AddressFromURIString("akka://sys@darkstar1:1110") +
        AddressFromURIString("akka://sys@darkstar3:1110") +
        AddressFromURIString("akka://sys@darkstar0:1110")

      val seq = addresses.toSeq
      seq.size should ===(4)
      seq(0) should ===(AddressFromURIString("akka://sys@darkstar0:1110"))
      seq(1) should ===(AddressFromURIString("akka://sys@darkstar1:1110"))
      seq(2) should ===(AddressFromURIString("akka://sys@darkstar2:1110"))
      seq(3) should ===(AddressFromURIString("akka://sys@darkstar3:1110"))
    }

    "order addresses by hostname and port" in {
      val addresses = SortedSet.empty[Address] +
        AddressFromURIString("akka://sys@darkstar2:1110") +
        AddressFromURIString("akka://sys@darkstar0:1111") +
        AddressFromURIString("akka://sys@darkstar2:1111") +
        AddressFromURIString("akka://sys@darkstar0:1110")

      val seq = addresses.toSeq
      seq.size should ===(4)
      seq(0) should ===(AddressFromURIString("akka://sys@darkstar0:1110"))
      seq(1) should ===(AddressFromURIString("akka://sys@darkstar0:1111"))
      seq(2) should ===(AddressFromURIString("akka://sys@darkstar2:1110"))
      seq(3) should ===(AddressFromURIString("akka://sys@darkstar2:1111"))
    }
  }

  "Leader status ordering" must {

    "order members with status Joining, Exiting and Down last" in {
      val address = Address("akka", "sys1", "host1", 5000)
      val m1 = m(address, Joining)
      val m2 = m(address.copy(port = Some(7000)), Joining)
      val m3 = m(address.copy(port = Some(3000)), Exiting)
      val m4 = m(address.copy(port = Some(6000)), Exiting)
      val m5 = m(address.copy(port = Some(2000)), Down)
      val m6 = m(address.copy(port = Some(4000)), Down)
      val m7 = m(address.copy(port = Some(8000)), Up)
      val m8 = m(address.copy(port = Some(9000)), Up)
      val expected = IndexedSeq(m7, m8, m1, m2, m3, m4, m5, m6)
      val shuffled = Random.shuffle(expected)
      shuffled.sorted(Member.leaderStatusOrdering) should ===(expected)
    }
  }
}
