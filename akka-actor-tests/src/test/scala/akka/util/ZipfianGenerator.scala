/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

/**
 * Zipfian generator algorithm from:
 * "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al.
 */
object ZipfianGenerator {
  final val DefaultTheta = 0.99
  final val DefaultSeed = 502539523

  def apply(min: Int, max: Int, theta: Double = DefaultTheta, seed: Int = DefaultSeed): ZipfianGenerator =
    new ZipfianGenerator(min, max, theta, seed)

  def apply(n: Int): ZipfianGenerator = ZipfianGenerator(min = 0, max = n - 1)

  private def zeta(n: Int, theta: Double): Double = {
    var sum = 0.0
    for (i <- 1 to n) {
      sum += 1 / Math.pow(i, theta)
    }
    sum
  }
}

/**
 * Zipfian generator algorithm from:
 * "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al.
 */
final class ZipfianGenerator(min: Int, max: Int, theta: Double, seed: Int) {
  private val n = max - min + 1
  private val alpha = 1.0 / (1.0 - theta)
  private val zeta2 = ZipfianGenerator.zeta(2, theta)
  private val zetaN = ZipfianGenerator.zeta(n, theta)
  private val eta = (1 - Math.pow(2.0 / n, 1 - theta)) / (1 - zeta2 / zetaN)
  private val random = new scala.util.Random(seed)

  def next(): Int = {
    val u = random.nextDouble()
    val uz = u * zetaN
    if (uz < 1.0) min
    else if (uz < 1.0 + Math.pow(0.5, theta)) min + 1
    else min + (n * Math.pow(eta * u - eta + 1, alpha)).toInt
  }
}
