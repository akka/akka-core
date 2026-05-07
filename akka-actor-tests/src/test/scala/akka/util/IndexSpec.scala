/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util.Comparator

import scala.concurrent.Await
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future
import scala.util.Random

import org.scalatest.matchers.should.Matchers

import akka.testkit.AkkaSpec
import akka.testkit.DefaultTimeout

class IndexSpec extends AkkaSpec with Matchers with DefaultTimeout {
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  private def emptyIndex =
    new Index[String, Int](100, new Comparator[Int] {
      override def compare(a: Int, b: Int): Int = Integer.compare(a, b)
    })

  private def indexWithValues = {
    val index = emptyIndex
    index.put("s1", 1)
    index.put("s1", 2)
    index.put("s1", 3)
    index.put("s2", 1)
    index.put("s2", 2)
    index.put("s3", 2)

    index
  }

  "An Index" must {

    "take and return a value" in {
      val index = emptyIndex
      index.put("s1", 1)
      index.valueIterator("s1").toSet should be(Set(1))
    }
    "take and return several values" in {
      val index = emptyIndex
      index.put("s1", 1) should ===(true)
      index.put("s1", 1) should ===(false)
      index.put("s1", 2)
      index.put("s1", 3)
      index.put("s2", 4)
      index.valueIterator("s1").toSet should ===(Set(1, 2, 3))
      index.valueIterator("s2").toSet should ===(Set(4))
    }
    "remove values" in {
      val index = emptyIndex
      index.put("s1", 1)
      index.put("s1", 2)
      index.put("s2", 1)
      index.put("s2", 2)
      //Remove value
      index.remove("s1", 1) should ===(true)
      index.remove("s1", 1) should ===(false)
      index.valueIterator("s1").toSet should ===(Set(2))
      //Remove key
      index.remove("s2") match {
        case Some(iter) => iter.toSet should ===(Set(1, 2))
        case None       => fail()
      }
      index.remove("s2") should ===(None)
      index.valueIterator("s2").toSet should ===(Set.empty[Int])
    }
    "remove the specified value" in {
      val index = emptyIndex
      index.put("s1", 1)
      index.put("s1", 2)
      index.put("s1", 3)
      index.put("s2", 1)
      index.put("s2", 2)
      index.put("s3", 2)

      index.removeValue(1)
      index.valueIterator("s1").toSet should ===(Set(2, 3))
      index.valueIterator("s2").toSet should ===(Set(2))
      index.valueIterator("s3").toSet should ===(Set(2))
    }
    "apply a function for all key-value pairs and find every value" in {
      val index = indexWithValues

      var valueCount = 0
      index.foreach((key, value) => {
        valueCount = valueCount + 1
        index.findValue(key)(_ == value) should ===(Some(value))
      })
      valueCount should ===(6)
    }
    "be cleared" in {
      val index = indexWithValues
      index.isEmpty should ===(false)
      index.clear()
      index.isEmpty should ===(true)
    }
    "be able to be accessed in parallel" in {
      val index = new Index[Int, Int](100, new Comparator[Int] {
        override def compare(a: Int, b: Int): Int = Integer.compare(a, b)
      })
      val nrOfTasks = 10000
      val nrOfKeys = 10
      val nrOfValues = 10
      //Fill index
      for (key <- 0 until nrOfKeys; value <- 0 until nrOfValues)
        index.put(key, value)
      //Tasks to be executed in parallel
      def putTask() = Future {
        index.put(Random.nextInt(nrOfKeys), Random.nextInt(nrOfValues))
      }
      def removeTask1() = Future {
        index.remove(Random.nextInt(nrOfKeys / 2), Random.nextInt(nrOfValues))
      }
      def removeTask2() = Future {
        index.remove(Random.nextInt(nrOfKeys / 2))
      }
      def readTask() = Future {
        val key = Random.nextInt(nrOfKeys)
        val values = index.valueIterator(key)
        if (key >= nrOfKeys / 2) {
          values.isEmpty should ===(false)
        }
      }

      def executeRandomTask() = Random.nextInt(4) match {
        case 0 => putTask()
        case 1 => removeTask1()
        case 2 => removeTask2()
        case 3 => readTask()
      }

      val tasks = List.fill(nrOfTasks)(executeRandomTask())

      tasks.foreach(Await.result(_, timeout.duration))
    }
  }
}
