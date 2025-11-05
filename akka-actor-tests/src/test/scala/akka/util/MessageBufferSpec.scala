/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.{ ActorPath, ActorRef, ActorRefProvider, MinimalActorRef }

class MessageBufferSpec extends AnyWordSpec with Matchers {

  import MessageBufferSpec._

  "A MessageBuffer" must {

    "answer empty correctly" in {
      val buffer = MessageBuffer.empty
      buffer.isEmpty should ===(true)
      buffer.nonEmpty should ===(false)
      buffer.append("m1", "s1")
      buffer.isEmpty should ===(false)
      buffer.nonEmpty should ===(true)
    }

    "append and drop" in {
      val buffer = MessageBuffer.empty
      buffer.size should ===(0)
      buffer.append("m1", "s1")
      buffer.size should ===(1)
      buffer.append("m2", "s2")
      buffer.size should ===(2)
      val (m1, s1) = buffer.head()
      buffer.size should ===(2)
      buffer.dropHead()
      buffer.size should ===(1)
      m1 should ===("m1")
      s1.toString should ===("s1")
      val (m2, s2) = buffer.head()
      buffer.dropHead()
      buffer.size should ===(0)
      buffer.dropHead()
      buffer.size should ===(0)
      m2 should ===("m2")
      s2.toString should ===("s2")
    }

    "process elements in the right order" in {
      val buffer = MessageBuffer.empty
      buffer.append("m1", "s1")
      buffer.append("m2", "s2")
      buffer.append("m3", "s3")
      val sb1 = new StringBuilder()
      buffer.foreach((m, s) => sb1.append(s"$m->$s:"))
      sb1.toString() should ===("m1->s1:m2->s2:m3->s3:")
      buffer.dropHead()
      val sb2 = new StringBuilder()
      buffer.foreach((m, s) => sb2.append(s"$m->$s:"))
      sb2.toString() should ===("m2->s2:m3->s3:")
    }

    "filterNot of first" in {
      val buffer = MessageBuffer.empty
      buffer.append("m1", "s1")
      buffer.append("m2", "s2")
      buffer.filterNot((m, _) => m == "m1")
      buffer.head()._1 should ===("m2")
      buffer.size should ===(1)
      buffer.filterNot((m, _) => m == "m2")
      buffer.isEmpty should ===(true)
    }

    "filterNot of last" in {
      val buffer = MessageBuffer.empty
      buffer.append("m1", "s1")
      buffer.append("m2", "s2")
      buffer.filterNot((m, _) => m == "m2")
      buffer.head()._1 should ===("m1")
      buffer.size should ===(1)
      buffer.filterNot((m, _) => m == "m1")
      buffer.isEmpty should ===(true)
    }

    "filterNot of single entry" in {
      val buffer = MessageBuffer.empty
      buffer.append("m1", "s1")
      buffer.append("m2", "s2")
      buffer.append("m3", "s3")
      buffer.append("m4", "s4")
      buffer.append("m5", "s5")
      buffer.append("m6", "s6")
      buffer.head()._1 should ===("m1")
      buffer.filterNot((m, _) => m == "m1")
      buffer.head()._1 should ===("m2")
      buffer.size should ===(5)
      buffer.filterNot((m, _) => m == "m3")
      buffer.size should ===(4)
      buffer.filterNot((m, _) => m == "m4")
      buffer.size should ===(3)
      buffer.dropHead()
      buffer.head()._1 should ===("m5")
      buffer.size should ===(2)
      buffer.filterNot((m, _) => m == "m6")
      buffer.size should ===(1)
      buffer.append("m7", "s7")
      buffer.head()._1 should ===("m5")
      buffer.size should ===(2)
      buffer.filterNot((m, _) => m == "m5")
      buffer.size should ===(1)
      buffer.filterNot((m, _) => m == "m7")
      buffer.size should ===(0)
      buffer.isEmpty should ===(true)
    }

    "filterNot of several entries" in {
      val buffer = MessageBuffer.empty
      buffer.append("m1", "s1")
      buffer.append("m2", "s2")
      buffer.append("m3", "s3")
      buffer.append("m4", "s4")
      buffer.append("m5", "s5")
      buffer.append("m6", "s6")
      buffer.filterNot((m, _) => m == "m1" || m == "m2")
      buffer.head()._1 should ===("m3")
      buffer.size should ===(4)
      buffer.filterNot((m, _) => m == "m4" || m == "m5")
      buffer.size should ===(2)
      buffer.dropHead()
      buffer.head()._1 should ===("m6")
      buffer.size should ===(1)
    }
  }

  "A MessageBufferMap" must {

    "support contains, add, append and remove" in {
      val map = new MessageBufferMap[String]
      map.contains("id1") should ===(false)
      map.getOrEmpty("id1").isEmpty should ===(true)
      map.totalSize should ===(0)
      map.add("id1")
      map.contains("id1") should ===(true)
      map.getOrEmpty("id1").isEmpty should ===(true)
      map.totalSize should ===(0)
      map.append("id1", "m1", "s1")
      map.contains("id1") should ===(true)
      map.getOrEmpty("id1").isEmpty should ===(false)
      map.totalSize should ===(1)
      map.remove("id1")
      map.contains("id1") should ===(false)
      map.getOrEmpty("id1").isEmpty should ===(true)
      map.totalSize should ===(0)
    }

    "handle multiple message buffers" in {
      val map = new MessageBufferMap[String]
      map.append("id1", "m11", "s11")
      map.append("id1", "m12", "s12")
      map.append("id2", "m21", "s21")
      map.append("id2", "m22", "s22")
      map.totalSize should ===(4)
      val sb = new StringBuilder()
      map.getOrEmpty("id1").foreach((m, s) => sb.append(s"id1->$m->$s:"))
      map.getOrEmpty("id2").foreach((m, s) => sb.append(s"id2->$m->$s:"))
      sb.toString() should ===("id1->m11->s11:id1->m12->s12:id2->m21->s21:id2->m22->s22:")
    }
  }
}

object MessageBufferSpec {
  final private[akka] class DummyActorRef(val id: String) extends MinimalActorRef {

    override def toString: String = id

    override def provider: ActorRefProvider = ???
    override def path: ActorPath = ???
  }

  import scala.language.implicitConversions
  implicit def string2ActorRef(s: String): ActorRef = new DummyActorRef(s)
}
