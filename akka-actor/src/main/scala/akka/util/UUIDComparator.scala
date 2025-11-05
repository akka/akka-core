/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util.Comparator
import java.util.UUID

/**
 * Scala implementation of UUIDComparator in
 * https://github.com/cowtowncoder/java-uuid-generator
 * Apache License 2.0.
 */
class UUIDComparator extends Comparator[UUID] {

  def compare(u1: UUID, u2: UUID): Int = {
    // First: major sorting by types
    val version = u1.version()
    val diff = version - u2.version()
    if (diff != 0) {
      diff
    } else {
      // Second: for time-based variant, order by time stamp:
      if (version == 1) {
        val diff2 = compareULongs(u1.timestamp(), u2.timestamp())
        if (diff2 == 0) {
          // or if that won't work, by other bits lexically
          compareULongs(u1.getLeastSignificantBits(), u2.getLeastSignificantBits())
        } else
          diff2
      } else {
        // note: java.util.Uuids compares with sign extension, IMO that's wrong, so:
        val diff2 = compareULongs(u1.getMostSignificantBits(), u2.getMostSignificantBits())
        if (diff2 == 0) {
          compareULongs(u1.getLeastSignificantBits(), u2.getLeastSignificantBits())
        } else
          diff2
      }
    }
  }

  private def compareULongs(l1: Long, l2: Long): Int = {
    val diff = compareUInts((l1 >> 32).toInt, (l2 >> 32).toInt)
    if (diff == 0)
      compareUInts(l1.toInt, l2.toInt)
    else
      diff
  }

  private def compareUInts(i1: Int, i2: Int): Int =
    /* bit messier due to java's insistence on signed values: if both
     * have same sign, normal comparison (by subtraction) works fine;
     * but if signs don't agree need to resolve differently
     */
    if (i1 < 0) {
      if (i2 < 0) (i1 - i2) else 1
    } else {
      if (i2 < 0) -1 else (i1 - i2)
    }

}

object UUIDComparator {
  val comparator = new UUIDComparator
}
