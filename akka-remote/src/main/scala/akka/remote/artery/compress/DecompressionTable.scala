/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.compress

/**
 * INTERNAL API
 *
 * @param version Either -1 for disabled or a version between 0 and 127
 */
private[remote] final case class DecompressionTable[T](originUid: Long, version: Byte, table: Array[T]) {

  private[this] val length = table.length

  def get(idx: Int): T = {
    if (idx >= length)
      throw new IllegalArgumentException(
        s"Attempted decompression of unknown id: [$idx]! " +
        s"Only $length ids allocated in table version [$version] for origin [$originUid].")
    table(idx)
  }

  def invert: CompressionTable[T] =
    CompressionTable(originUid, version, table.zipWithIndex.toMap)

  /** Writes complete table as String (heavy operation) */
  override def toString =
    s"DecompressionTable($originUid, $version, " +
    s"Map(${table.zipWithIndex.map({ case (t, i) => s"$i -> $t" }).mkString(",")}))"
}

/** INTERNAL API */
private[remote] object DecompressionTable {

  val DisabledVersion: Byte = -1

  private[this] val _empty = DecompressionTable(0, 0, Array.empty[Any])
  def empty[T] = _empty.asInstanceOf[DecompressionTable[T]]
  def disabled[T] = empty[T].copy(version = DisabledVersion)
}
