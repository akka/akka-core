/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.net.URLEncoder

import scala.collection.immutable

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RelativeActorPathSpec extends AnyWordSpec with Matchers {

  def elements(path: String): immutable.Seq[String] = RelativeActorPath.unapply(path).getOrElse(Nil)

  "RelativeActorPath" must {
    "match single name" in {
      elements("foo") should ===(List("foo"))
    }
    "match path separated names" in {
      elements("foo/bar/baz") should ===(List("foo", "bar", "baz"))
    }
    "match url encoded name" in {
      val name = URLEncoder.encode("akka://ClusterSystem@127.0.0.1:2552", "UTF-8")
      elements(name) should ===(List(name))
    }
    "match path with uid fragment" in {
      elements("foo/bar/baz#1234") should ===(List("foo", "bar", "baz#1234"))
    }
  }
}
