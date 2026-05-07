/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import akka.pattern.RetrySettings.ExponentialBackoffFunction
import akka.pattern.RetrySettings.FixedDelayFunction
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.DurationInt

class RetrySettingsSpec extends AnyWordSpec with Matchers {

  "RetrySettings" should {
    "create with default values" in {
      {
        val retrySettings = RetrySettings(2)

        retrySettings.maxRetries shouldBe 2
        val exp = retrySettings.delayFunction.asInstanceOf[ExponentialBackoffFunction]
        exp.minBackoff shouldBe 100.millis
        exp.maxBackoff shouldBe 400.millis
        exp.randomFactor should ===(0.2 +- 1e-6)
      }
      {
        val retrySettings = RetrySettings(4)

        retrySettings.maxRetries shouldBe 4
        val exp = retrySettings.delayFunction.asInstanceOf[ExponentialBackoffFunction]
        exp.minBackoff shouldBe 100.millis
        exp.maxBackoff shouldBe 1600.millis
        exp.randomFactor should ===(0.3 +- 1e-6)
      }
      {
        val retrySettings = RetrySettings(8)

        retrySettings.maxRetries shouldBe 8
        val exp = retrySettings.delayFunction.asInstanceOf[ExponentialBackoffFunction]
        exp.minBackoff shouldBe 100.millis
        exp.maxBackoff shouldBe 25600.millis
        exp.randomFactor should ===(0.5 +- 1e-6)
      }
      {
        val retrySettings = RetrySettings(10)

        retrySettings.maxRetries shouldBe 10
        val exp = retrySettings.delayFunction.asInstanceOf[ExponentialBackoffFunction]
        exp.minBackoff shouldBe 100.millis
        exp.maxBackoff shouldBe 1.minute
        exp.randomFactor should ===(0.6 +- 1e-6)
      }
      {
        val retrySettings = RetrySettings(100)

        retrySettings.maxRetries shouldBe 100
        val exp = retrySettings.delayFunction.asInstanceOf[ExponentialBackoffFunction]
        exp.minBackoff shouldBe 100.millis
        exp.maxBackoff shouldBe 1.minute
        exp.randomFactor should ===(1.0 +- 1e-6)
      }
    }

    "override default values" in {
      val retrySettings = RetrySettings(2).withExponentialBackoff(1.second, 2.seconds, 0.5)

      retrySettings.maxRetries shouldBe 2
      val exp = retrySettings.delayFunction.asInstanceOf[ExponentialBackoffFunction]
      exp.minBackoff shouldBe 1.second
      exp.maxBackoff shouldBe 2.seconds
      exp.randomFactor shouldBe 0.5
    }

    "create from config" in {
      {
        val retrySettings = RetrySettings(ConfigFactory.parseString("max-retries = 2"))
        retrySettings.maxRetries shouldBe 2
        val exp = retrySettings.delayFunction.asInstanceOf[ExponentialBackoffFunction]
        exp.minBackoff shouldBe 100.millis
        exp.maxBackoff shouldBe 400.millis
        exp.randomFactor should ===(0.2 +- 1e-6)
      }
      {
        val retrySettings = RetrySettings(ConfigFactory.parseString("""
            |max-retries = 2
            |fixed-delay = 500ms
            |""".stripMargin))
        retrySettings.maxRetries shouldBe 2
        retrySettings.delayFunction.asInstanceOf[FixedDelayFunction].delay shouldBe 500.millis
      }
      {
        val retrySettings = RetrySettings(ConfigFactory.parseString("""
                                                                      |max-retries = 2
                                                                      |min-backoff = 500ms
                                                                      |max-backoff = 1s
                                                                      |random-factor = 0.5
                                                                      |""".stripMargin))
        retrySettings.maxRetries shouldBe 2
        val exp = retrySettings.delayFunction.asInstanceOf[ExponentialBackoffFunction]
        exp.minBackoff shouldBe 500.millis
        exp.maxBackoff shouldBe 1.second
        exp.randomFactor should ===(0.5 +- 1e-6)
      }
    }
  }
}
