/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.discovery.config

import scala.collection.immutable

import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.discovery.ServiceDiscovery.{ Resolved, ResolvedTarget }
import akka.discovery.config.ConfigServicesParserSpec._

object ConfigServicesParserSpec {
  val exampleConfig: Config = ConfigFactory.parseString("""
      services {
        service1 {
          endpoints = [
            {
              host = "cat"
              port = 1233
            },
            {
              host = "dog"
            }
          ]
        },
        service2 {
          endpoints = []
        }
      }
    """.stripMargin)
}

class ConfigServicesParserSpec extends AnyWordSpec with Matchers {

  "Config parsing" must {
    "parse" in {
      val config = exampleConfig.getConfig("services")

      val result = ConfigServicesParser.parse(config)

      result("service1") shouldEqual Resolved(
        "service1",
        immutable.Seq(
          ResolvedTarget(host = "cat", port = Some(1233), address = None),
          ResolvedTarget(host = "dog", port = None, address = None)))
      result("service2") shouldEqual Resolved("service2", immutable.Seq())
    }
  }
}
