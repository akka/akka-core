/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.collection.{ immutable => im }

import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class JoinConfigCompatPreDefinedChecksSpec extends AnyWordSpec with Matchers {

  // Test for some of the pre-build helpers we offer
  "JoinConfigCompatChecker.exists" must {

    val requiredKeys = im.Seq(
      "akka.cluster.min-nr-of-members",
      "akka.cluster.retry-unsuccessful-join-after",
      "akka.cluster.allow-weakly-up-members")

    "pass when all required keys are provided" in {

      val result =
        JoinConfigCompatChecker.exists(
          requiredKeys,
          config("""
              |{
              | akka.cluster.min-nr-of-members = 1
              | akka.cluster.retry-unsuccessful-join-after = 10s
              | akka.cluster.allow-weakly-up-members = on
              |}
            """.stripMargin))

      result shouldBe Valid
    }

    "fail when some required keys are NOT provided" in {

      JoinConfigCompatChecker.exists(
        requiredKeys,
        config("""
            |{
            | akka.cluster.min-nr-of-members = 1
            |}
          """.stripMargin)) match {
        case Valid =>
          fail()
        case Invalid(incompatibleKeys) =>
          incompatibleKeys should have size 2
          incompatibleKeys should contain("akka.cluster.retry-unsuccessful-join-after is missing")
          incompatibleKeys should contain("akka.cluster.allow-weakly-up-members is missing")
      }
    }
  }

  "JoinConfigCompatChecker.fullMatch" must {

    val requiredKeys = im.Seq(
      "akka.cluster.min-nr-of-members",
      "akka.cluster.retry-unsuccessful-join-after",
      "akka.cluster.allow-weakly-up-members")

    val clusterConfig =
      config("""
          |{
          | akka.cluster.min-nr-of-members = 1
          | akka.cluster.retry-unsuccessful-join-after = 10s
          | akka.cluster.allow-weakly-up-members = on
          |}
        """.stripMargin)

    "pass when all required keys are provided and all match cluster config" in {

      val result =
        JoinConfigCompatChecker.fullMatch(
          requiredKeys,
          config("""
              |{
              | akka.cluster.min-nr-of-members = 1
              | akka.cluster.retry-unsuccessful-join-after = 10s
              | akka.cluster.allow-weakly-up-members = on
              |}
            """.stripMargin),
          clusterConfig)

      result shouldBe Valid
    }

    "fail when some required keys are NOT provided" in {

      JoinConfigCompatChecker.fullMatch(
        requiredKeys,
        config("""
            |{
            | akka.cluster.min-nr-of-members = 1
            |}
          """.stripMargin),
        clusterConfig) match {
        case Valid =>
          fail()
        case Invalid(incompatibleKeys) =>
          incompatibleKeys should have size 2
          incompatibleKeys should contain("akka.cluster.retry-unsuccessful-join-after is missing")
          incompatibleKeys should contain("akka.cluster.allow-weakly-up-members is missing")
      }
    }

    "fail when all required keys are passed, but some values don't match cluster config" in {

      JoinConfigCompatChecker.fullMatch(
        requiredKeys,
        config("""
            |{
            | akka.cluster.min-nr-of-members = 1
            | akka.cluster.retry-unsuccessful-join-after = 15s
            | akka.cluster.allow-weakly-up-members = off
            |}
          """.stripMargin),
        clusterConfig) match {
        case Valid =>
          fail()
        case Invalid(incompatibleKeys) =>
          incompatibleKeys should have size 2
          incompatibleKeys should contain("akka.cluster.retry-unsuccessful-join-after is incompatible")
          incompatibleKeys should contain("akka.cluster.allow-weakly-up-members is incompatible")
      }
    }

    "fail when all required keys are passed, but some are missing and others don't match cluster config" in {

      JoinConfigCompatChecker.fullMatch(
        requiredKeys,
        config("""
            |{
            | akka.cluster.min-nr-of-members = 1
            | akka.cluster.allow-weakly-up-members = off
            |}
          """.stripMargin),
        clusterConfig) match {
        case Valid =>
          fail()
        case Invalid(incompatibleKeys) =>
          incompatibleKeys should have size 2
          incompatibleKeys should contain("akka.cluster.retry-unsuccessful-join-after is missing")
          incompatibleKeys should contain("akka.cluster.allow-weakly-up-members is incompatible")
      }
    }
  }

  def config(str: String): Config = ConfigFactory.parseString(str).resolve()

}
