/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import scala.util.Random

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.stream.impl.ResizableMultiReaderRingBuffer._

class ResizableMultiReaderRingBufferSpec extends AnyWordSpec with Matchers {

  "A ResizableMultiReaderRingBuffer" should {

    "initially be empty (1)" in new Test(iSize = 2, mSize = 4, cursorCount = 1) {
      inspect shouldEqual "0 0 (size=0, writeIx=0, readIx=0, cursors=1)"
    }

    "initially be empty (2)" in new Test(iSize = 4, mSize = 4, cursorCount = 3) {
      inspect shouldEqual "0 0 0 0 (size=0, writeIx=0, readIx=0, cursors=3)"
    }

    "fail reads if nothing can be read" in new Test(iSize = 4, mSize = 4, cursorCount = 3) {
      write(1) shouldEqual true
      write(2) shouldEqual true
      write(3) shouldEqual true
      inspect shouldEqual "1 2 3 0 (size=3, writeIx=3, readIx=0, cursors=3)"
      read(0) shouldEqual 1
      read(0) shouldEqual 2
      read(1) shouldEqual 1
      inspect shouldEqual "1 2 3 0 (size=3, writeIx=3, readIx=0, cursors=3)"
      read(0) shouldEqual 3
      read(0) shouldEqual null
      read(1) shouldEqual 2
      inspect shouldEqual "1 2 3 0 (size=3, writeIx=3, readIx=0, cursors=3)"
      read(2) shouldEqual 1
      inspect shouldEqual "0 2 3 0 (size=2, writeIx=3, readIx=1, cursors=3)"
      read(1) shouldEqual 3
      read(1) shouldEqual null
      read(2) shouldEqual 2
      read(2) shouldEqual 3
      inspect shouldEqual "0 0 0 0 (size=0, writeIx=3, readIx=3, cursors=3)"
    }

    "fail writes if there is no more space" in new Test(iSize = 4, mSize = 4, cursorCount = 2) {
      write(1) shouldEqual true
      write(2) shouldEqual true
      write(3) shouldEqual true
      write(4) shouldEqual true
      write(5) shouldEqual false
      read(0) shouldEqual 1
      write(5) shouldEqual false
      read(1) shouldEqual 1
      write(5) shouldEqual true
      read(0) shouldEqual 2
      read(0) shouldEqual 3
      read(0) shouldEqual 4
      read(1) shouldEqual 2
      write(6) shouldEqual true
      inspect shouldEqual "5 6 3 4 (size=4, writeIx=6, readIx=2, cursors=2)"
      read(0) shouldEqual 5
      read(0) shouldEqual 6
      read(0) shouldEqual null
      read(1) shouldEqual 3
      read(1) shouldEqual 4
      read(1) shouldEqual 5
      read(1) shouldEqual 6
      read(1) shouldEqual null
      inspect shouldEqual "0 0 0 0 (size=0, writeIx=6, readIx=6, cursors=2)"
      write(7) shouldEqual true
      write(8) shouldEqual true
      write(9) shouldEqual true
      inspect shouldEqual "9 0 7 8 (size=3, writeIx=9, readIx=6, cursors=2)"
      read(0) shouldEqual 7
      read(0) shouldEqual 8
      read(0) shouldEqual 9
      read(0) shouldEqual null
      read(1) shouldEqual 7
      read(1) shouldEqual 8
      read(1) shouldEqual 9
      read(1) shouldEqual null
      inspect shouldEqual "0 0 0 0 (size=0, writeIx=9, readIx=9, cursors=2)"
    }

    "automatically grow if possible" in new Test(iSize = 2, mSize = 8, cursorCount = 2) {
      write(1) shouldEqual true
      inspect shouldEqual "1 0 (size=1, writeIx=1, readIx=0, cursors=2)"
      write(2) shouldEqual true
      inspect shouldEqual "1 2 (size=2, writeIx=2, readIx=0, cursors=2)"
      write(3) shouldEqual true
      inspect shouldEqual "1 2 3 0 (size=3, writeIx=3, readIx=0, cursors=2)"
      write(4) shouldEqual true
      inspect shouldEqual "1 2 3 4 (size=4, writeIx=4, readIx=0, cursors=2)"
      read(0) shouldEqual 1
      read(0) shouldEqual 2
      read(0) shouldEqual 3
      read(1) shouldEqual 1
      read(1) shouldEqual 2
      write(5) shouldEqual true
      inspect shouldEqual "5 0 3 4 (size=3, writeIx=5, readIx=2, cursors=2)"
      write(6) shouldEqual true
      inspect shouldEqual "5 6 3 4 (size=4, writeIx=6, readIx=2, cursors=2)"
      write(7) shouldEqual true
      inspect shouldEqual "3 4 5 6 7 0 0 0 (size=5, writeIx=5, readIx=0, cursors=2)"
      read(0) shouldEqual 4
      read(0) shouldEqual 5
      read(0) shouldEqual 6
      read(0) shouldEqual 7
      read(0) shouldEqual null
      read(1) shouldEqual 3
      read(1) shouldEqual 4
      read(1) shouldEqual 5
      read(1) shouldEqual 6
      read(1) shouldEqual 7
      read(1) shouldEqual null
      inspect shouldEqual "0 0 0 0 0 0 0 0 (size=0, writeIx=5, readIx=5, cursors=2)"
    }

    "pass the stress test" in {
      // create 100 buffers with an initialSize of 1 and a maxSize of 1 to 64,
      // for each one attach 1 to 8 cursors and randomly try reading and writing to the buffer;
      // in total 200 elements need to be written to the buffer and read in the correct order by each cursor
      val MAXSIZEBIT_LIMIT = 6 // 2 ^ (this number)
      val COUNTER_LIMIT = 200
      val LOG = false
      val sb = new java.lang.StringBuilder
      def log(s: => String): Unit = if (LOG) sb.append(s)

      class StressTestCursor(cursorNr: Int, run: Int) extends Cursor {
        var cursor: Int = _
        var counter = 1
        def tryReadAndReturnTrueIfDone(buf: TestBuffer): Boolean = {
          log(s"  Try reading of $toString: ")
          try {
            val x = buf.read(this)
            log("OK\n")
            if (x != counter)
              fail(s"""|Run $run, cursorNr $cursorNr, counter $counter: got unexpected $x
                         |  Buf: ${buf.inspect}
                         |  Cursors: ${buf.cursors.cursors.mkString("\n           ")}
                         |Log:\n$sb
                      """.stripMargin)
            counter += 1
            counter == COUNTER_LIMIT
          } catch {
            case NothingToReadException => log("FAILED\n"); false // ok, we currently can't read, try again later
          }
        }
        override def toString: String = s"cursorNr $cursorNr, ix $cursor, counter $counter"
      }

      val random = new Random
      for {
        bit <- 1 to MAXSIZEBIT_LIMIT
        _ <- 1 to 2
      } {
        var counter = 1
        var activeCursors = List.tabulate(random.nextInt(8) + 1)(new StressTestCursor(_, 1 << bit))
        var stillWriting = 2 // give writing a slight bias, so as to somewhat "stretch" the buffer
        val buf = new TestBuffer(1, 1 << bit, new Cursors { def cursors = activeCursors })
        sb.setLength(0)
        while (activeCursors.nonEmpty) {
          log(s"Buf: ${buf.inspect}\n")
          val activeCursorCount = activeCursors.size
          val index = random.nextInt(activeCursorCount + stillWriting)
          if (index >= activeCursorCount) {
            log(s"  Writing $counter: ")
            if (buf.write(counter)) {
              log("OK\n")
              counter += 1
            } else {
              log("FAILED\n")
              if (counter == COUNTER_LIMIT) stillWriting = 0
            }
          } else {
            val cursor = activeCursors(index)
            if (cursor.tryReadAndReturnTrueIfDone(buf))
              activeCursors = activeCursors.filter(_ != cursor)
          }
        }
      }
    }
  }

  class TestBuffer(iSize: Int, mSize: Int, cursors: Cursors)
      extends ResizableMultiReaderRingBuffer[Int](iSize, mSize, cursors) {
    def inspect: String =
      underlyingArray.map(x => if (x == null) 0 else x).mkString("", " ", " " + toString.dropWhile(_ != '('))
  }

  class Test(iSize: Int, mSize: Int, cursorCount: Int)
      extends TestBuffer(iSize, mSize, new SimpleCursors(cursorCount)) {
    def read(cursorIx: Int): Integer =
      try read(cursors.cursors(cursorIx))
      catch { case NothingToReadException => null }
  }

  class SimpleCursors(cursorCount: Int) extends Cursors {
    val cursors: List[Cursor] = List.fill(cursorCount)(new Cursor { var cursor: Int = _ })
  }
}
