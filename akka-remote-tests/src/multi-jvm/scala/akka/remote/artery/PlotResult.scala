/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

final case class PlotResult(values: Vector[(String, Number)] = Vector.empty) {

  def add(key: String, value: Number): PlotResult =
    copy(values = values :+ (key -> value))

  def addAll(p: PlotResult): PlotResult =
    copy(values ++ p.values)

  def csvLabels: String = values.map(_._1).mkString("\"", "\",\"", "\"")

  def csvValues: String = values.map(_._2).mkString("\"", "\",\"", "\"")

  // this can be split to two lines with bash: cut -d':' -f2,3 | tr ':' $'\n'
  def csv(name: String): String = s"PLOT_${name}:${csvLabels}:${csvValues}"

}

final case class LatencyPlots(
    plot50: PlotResult = PlotResult(),
    plot90: PlotResult = PlotResult(),
    plot99: PlotResult = PlotResult())
