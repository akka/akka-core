/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.passivation.simulator

import scala.concurrent.Future

import akka.stream.scaladsl.Sink

object SimulatorStats {
  def apply(): Sink[Simulator.Event, Future[ShardingStats]] =
    Sink.fold(ShardingStats()) {
      case (stats, Simulator.Accessed(regionId, shardId, _)) =>
        stats.accessed(regionId, shardId)
      case (stats, Simulator.Activated(regionId, shardId, _)) =>
        stats.activated(regionId, shardId)
      case (stats, Simulator.Passivated(regionId, shardId, entityIds)) =>
        stats.passivated(regionId, shardId, entityIds.size)
    }
}

final case class EntityStats(accesses: Long = 0, activations: Long = 0, passivations: Long = 0) {
  def accessed(): EntityStats = copy(accesses + 1, activations, passivations)
  def activated(): EntityStats = copy(accesses, activations + 1, passivations)
  def passivated(n: Int): EntityStats = copy(accesses, activations, passivations + n)

  def activePercent: Float = (1 - (activations.toFloat / accesses)) * 100

  def ++(other: EntityStats): EntityStats =
    EntityStats(accesses + other.accesses, activations + other.activations, passivations + other.passivations)
}

final case class RegionStats(shardStats: Map[String, EntityStats] = Map.empty) {
  def accessed(shardId: String): RegionStats =
    RegionStats(shardStats.updated(shardId, shardStats.getOrElse(shardId, EntityStats()).accessed()))

  def activated(shardId: String): RegionStats =
    RegionStats(shardStats.updated(shardId, shardStats.getOrElse(shardId, EntityStats()).activated()))

  def passivated(shardId: String, n: Int): RegionStats =
    RegionStats(shardStats.updated(shardId, shardStats.getOrElse(shardId, EntityStats()).passivated(n)))

  def totals: EntityStats = shardStats.values.foldLeft(EntityStats())(_ ++ _)
}

final case class ShardingStats(regionStats: Map[String, RegionStats] = Map.empty) {
  def accessed(regionId: String, shardId: String): ShardingStats =
    ShardingStats(regionStats.updated(regionId, regionStats.getOrElse(regionId, RegionStats()).accessed(shardId)))

  def activated(regionId: String, shardId: String): ShardingStats =
    ShardingStats(regionStats.updated(regionId, regionStats.getOrElse(regionId, RegionStats()).activated(shardId)))

  def passivated(regionId: String, shardId: String, n: Int): ShardingStats =
    ShardingStats(regionStats.updated(regionId, regionStats.getOrElse(regionId, RegionStats()).passivated(shardId, n)))

  def ++(other: ShardingStats): ShardingStats =
    ShardingStats(regionStats ++ other.regionStats)

  def totals: EntityStats = regionStats.values.foldLeft(EntityStats())(_ ++ _.totals)
}

case class DataTable(headers: DataTable.Row, rows: Seq[DataTable.Row])

object DataTable {
  type Row = IndexedSeq[String]

  object Headers {
    val EntityStats: Row = IndexedSeq("Active", "Accesses", "Activations", "Passivations")
    val RegionStats: Row = "Shard" +: EntityStats
    val ShardingStats: Row = "Region" +: RegionStats
    val RunStats: Row = "Run" +: EntityStats
  }

  def apply(stats: EntityStats): DataTable =
    DataTable(Headers.EntityStats, Seq(row(stats)))

  def row(stats: EntityStats): Row =
    IndexedSeq(
      f"${stats.activePercent}%.2f %%",
      f"${stats.accesses}%,d",
      f"${stats.activations}%,d",
      f"${stats.passivations}%,d")

  def apply(stats: RegionStats): DataTable =
    DataTable(Headers.RegionStats, stats.shardStats.toSeq.sortBy(_._1).flatMap {
      case (shardId, stats) => DataTable(stats).rows.map(shardId +: _)
    })

  def apply(stats: ShardingStats): DataTable =
    DataTable(Headers.ShardingStats, stats.regionStats.toSeq.sortBy(_._1).flatMap {
      case (regionId, stats) => DataTable(stats).rows.map(regionId +: _)
    })
}

object PrintData {
  def apply(data: DataTable): Unit = {
    val columnWidths = determineColumnWidths(data)
    val builder = new StringBuilder
    builder ++= topDivider(columnWidths)
    builder ++= line(data.headers, columnWidths)
    data.rows.take(1).foreach { row =>
      builder ++= headerDivider(columnWidths)
      builder ++= line(row, columnWidths)
    }
    data.rows.drop(1).foreach { rowData =>
      builder ++= rowDivider(columnWidths)
      builder ++= line(rowData, columnWidths)
    }
    builder ++= bottomDivider(columnWidths)
    builder ++= "\n"
    println(builder.result())
  }

  private def determineColumnWidths(data: DataTable): IndexedSeq[Int] = {
    val allRows = data.headers +: data.rows
    data.headers.indices.map(i => allRows.map(row => row(i).length).max)
  }

  private def topDivider(columnWidths: Seq[Int]): String =
    divider("╔", "═", "╤", "╗", columnWidths)

  private def headerDivider(columnWidths: Seq[Int]): String =
    divider("╠", "═", "╪", "╣", columnWidths)

  private def rowDivider(columnWidths: Seq[Int]): String =
    divider("╟", "─", "┼", "╢", columnWidths)

  private def bottomDivider(columnWidths: Seq[Int]): String =
    divider("╚", "═", "╧", "╝", columnWidths)

  private def divider(start: String, line: String, separator: String, end: String, columnWidths: Seq[Int]): String =
    columnWidths.map(width => line * (width + 2)).mkString(start, separator, end) + "\n"

  private def line(row: DataTable.Row, columnWidths: Seq[Int]): String =
    row.zip(columnWidths).map({ case (cell, width) => pad(cell, width) }).mkString("║ ", " │ ", " ║") + "\n"

  private def pad(string: String, width: Int): String =
    " " * (width - string.length) + string
}
