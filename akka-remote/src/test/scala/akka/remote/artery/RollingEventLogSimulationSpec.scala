/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import scala.annotation.tailrec
import scala.util.Random
import scala.util.control.NonFatal

import akka.testkit.AkkaSpec

/*
 * This test is a simulation of the actual concurrent rolling log implemented in SnapshottableRollingEventLog. It
 * is not possible to test the implementation to such extent than this simulation allows, however, the two implementations
 * must be kept in sync manually (it is expected to remain simple though).
 *
 * It is very important to not get corrupted results from the Flight Recorder as they can lead to completely misinterpreted
 * results when debugging using the logs. This simulation tries to uncover many race scenarios by simulating the
 * algorithm down to the individual byte write level.
 */
class RollingEventLogSimulationSpec extends AkkaSpec {

  val Committed: Byte = 0.toByte
  val Dirty: Byte = 1.toByte

  val letterCodes = Array("A", "B", "C", "D", "E", "F")
  val EntrySize = 4

  class Writer(writerId: Int, entryCount: Int, totalWrites: Int) {
    val letterCode = letterCodes(writerId)
    val bufSize = entryCount * EntrySize

    // Models an instruction that does read or write to some shared location
    sealed trait Instruction {
      def apply(simulator: Simulator): String
    }

    // getAndIncrement on the header and store it in local variable
    case object AdvanceHeader extends Instruction {
      override def apply(simulator: Simulator): String = {
        seenHeader = simulator.headPointer
        slot = seenHeader % bufSize
        simulator.headPointer += EntrySize
        writePointer = slot + 1 // Leave one byte for the commit header
        advance()
        s"$letterCode sees header $seenHeader advances it to ${simulator.headPointer}"
      }
    }

    // CAS on the commit status field, if fails jump to start of loop
    case object TryMarkDirty extends Instruction {
      override def apply(simulator: Simulator): String = {
        if (simulator.simulatedBuffer(slot) == Dirty) {
          instructionPtr = 0 // Retry loop
          s"$letterCode sees dirty record at $seenHeader, retries"
        } else {
          simulator.simulatedBuffer(slot) = Dirty
          advance()
          s"$letterCode sees committed record at $seenHeader, proceeds"
        }
      }
    }

    // This step is just to be able to do consistency checks. Simply writes the ID of the writer as the first
    // byte of the record.
    case object WriteId extends Instruction {
      override def apply(simulator: Simulator): String = {
        simulator.simulatedBuffer(writePointer) = writerId.toByte // Avoid zero since we start from zeroed buf
        writePointer += 1
        advance()
        s"$letterCode writes ID to offset ${writePointer - 1}"
      }
    }

    // Write an individual byte to the record. Visibility issues are not modeled, but they are likely relevant
    // since writing Commit will be the proper barrier anyway.
    case object WriteByte extends Instruction {
      override def apply(simulator: Simulator): String = {
        simulator.simulatedBuffer(writePointer) = (writeCount + 1).toByte // Avoid zero since we start from zeroed buf
        writePointer += 1
        advance()
        s"$letterCode writes byte ${writeCount + 1} to offset ${writePointer - 1}"
      }
    }

    // Sets the commit status to Committed
    case object Commit extends Instruction {
      override def apply(simulator: Simulator): String = {
        simulator.simulatedBuffer(slot) = Committed
        advance()
        s"$letterCode commits at $seenHeader"
      }
    }

    var instructionPtr = 0
    var writeCount = 0
    var seenHeader = 0
    var slot = 0
    var writePointer = 0

    val instructions: Array[Instruction] =
      (Array(AdvanceHeader, TryMarkDirty) :+
      WriteId) ++
      Array.fill(EntrySize - 2)(WriteByte) :+
      Commit

    def step(simulator: Simulator): String = {
      instructions(instructionPtr)(simulator)
    }

    private def advance(): Unit = {
      instructionPtr += 1
      if (instructionPtr == instructions.size) {
        instructionPtr = 0
        writeCount += 1
      }
    }

    def isFinished: Boolean = writeCount == totalWrites

  }

  class Simulator(writerCount: Int, entryCount: Int, totalWrites: Int) {
    var headPointer = 0
    val simulatedBuffer = new Array[Byte](4 * entryCount)
    val writers = Array.tabulate(writerCount)(new Writer(_, entryCount, totalWrites))
    var activeWriters = writerCount
    var log: List[String] = Nil

    @tailrec private def chooseWriter: Writer = {
      val idx = Random.nextInt(writerCount)
      val writer = writers(idx)
      if (writer.isFinished) chooseWriter
      else writer
    }

    def run(): Unit = {
      try {
        while (activeWriters > 0) {
          val writer = chooseWriter
          val event = writer.step(this)
          log ::= event
          if (writer.isFinished) activeWriters -= 1
          consistencyChecks()
        }
      } catch {
        case NonFatal(e) =>
          println(log.reverse.mkString("\n"))
          println("----------- BUFFER CONTENT -------------")
          println(simulatedBuffer.grouped(EntrySize).map(_.mkString("[", ",", "]")).mkString(", "))
          throw e
      }
      allRecordsCommitted()
    }

    def consistencyChecks(): Unit = {
      checkNoPartialWrites()
      checkGaplessWrites()
    }

    // No Committed records should contain bytes from two different writes (Dirty records might, though).
    def checkNoPartialWrites(): Unit = {
      for (entry <- 0 until entryCount if simulatedBuffer(entry * EntrySize) == Committed) {
        val ofs = entry * EntrySize
        if (simulatedBuffer(ofs + 2) != simulatedBuffer(ofs + 3))
          fail(s"Entry $entry is corrupted, partial writes are visible")
      }
    }

    // All writes for a given ID must:
    // - contain the last write, or no writes at all
    // - any writes in addition to the last write should be gapless (but possibly truncated)
    // good examples (showing the write numbers, assuming latest is 4):
    // [2, 3, 4]
    // [4]
    // []
    // [3, 4]
    // bad examples
    // [2, 3]
    // [2, 4]
    def checkGaplessWrites(): Unit = {
      for (id <- 0 until writerCount) {
        val writeCount = writers(id).writeCount
        val lastWrittenSlot = (headPointer - EntrySize) % EntrySize
        var nextExpected = writeCount
        val totalWrittenEntries = headPointer % EntrySize

        for (i <- 0 until math.min(entryCount, totalWrittenEntries)) {
          val slot = (entryCount + lastWrittenSlot - i) % entryCount
          val offs = slot * EntrySize
          if (simulatedBuffer(offs) == Committed && simulatedBuffer(offs + 1) == id) {
            if (simulatedBuffer(offs + 2) != nextExpected)
              fail(s"Entry $slot is corrupted, contains write ${simulatedBuffer(offs + 2)} but expected $nextExpected")
            nextExpected -= 1
          }
        }
      }
    }

    def allRecordsCommitted(): Unit = {
      for (entry <- 0 until entryCount) {
        if (simulatedBuffer(entry * EntrySize) != Committed)
          fail(s"Entry $entry is not Committed")
      }
    }
  }

  "RollingEventLog algorithm" must {

    "ensure write consistency in simulation" in {
      // 600 record writes, roughly 3600 instructions in total, racing for 32 memory locations (plus the head pointer)
      val sim = new Simulator(writerCount = 6, entryCount = 8, totalWrites = 100)
      sim.run()
    }

  }

}
