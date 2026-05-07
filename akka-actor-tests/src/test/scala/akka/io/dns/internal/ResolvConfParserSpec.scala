/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns.internal

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ResolvConfParserSpec extends AnyWordSpec with Matchers {

  private def parse(str: String): ResolvConf = {
    ResolvConfParser.parseLines(str.linesIterator)
  }

  "The ResolvConfParser" should {

    "parse an actual Kubernetes resolv.conf file" in {
      val resolvConf = parse("""nameserver 172.30.0.2
          |search myproject.svc.cluster.local svc.cluster.local cluster.local
          |options ndots:5""".stripMargin)
      resolvConf.search should be(List("myproject.svc.cluster.local", "svc.cluster.local", "cluster.local"))
      resolvConf.ndots should be(5)
    }

    "ignore # comments" in {
      parse("""search example.com
          |#search foobar.com""".stripMargin).search should be(List("example.com"))
    }

    "ignore ; comments" in {
      parse("""search example.com
          |;search foobar.com""".stripMargin).search should be(List("example.com"))
    }

    "use the last search element found" in {
      parse("""search example.com
          |search foobar.com""".stripMargin).search should be(List("foobar.com"))
    }

    "support domain elements" in {
      parse("domain example.com").search should be(List("example.com"))
    }

    "use the last domain element found" in {
      parse("""domain example.com
          |domain foobar.com
        """.stripMargin).search should be(List("foobar.com"))
    }

    "ignore non ndots options" in {
      parse("options\trotate\tinet6\tndots:3\tattempts:4").ndots should be(3)
    }

    "ignore tabs and spaces" in {
      parse("  \t \n \t domain \t \texample.com  \t \t\n\t\t  ").search should be(List("example.com"))
    }

    "default to ndots 1" in {
      parse("").ndots should be(1)
    }
  }
}
