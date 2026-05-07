/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.net.MalformedURLException

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ActorPathSpec extends AnyWordSpec with Matchers {

  "An ActorPath" must {

    "support parsing its String rep" in {
      val path = RootActorPath(Address("akka", "mysys")) / "user"
      ActorPath.fromString(path.toString) should ===(path)
    }

    "support parsing remote paths" in {
      val remote = "akka://my_sys@host:1234/some/ref"
      ActorPath.fromString(remote).toString should ===(remote)
    }

    "throw exception upon malformed paths" in {
      intercept[MalformedURLException] { ActorPath.fromString("") }
      intercept[MalformedURLException] { ActorPath.fromString("://hallo") }
      intercept[MalformedURLException] { ActorPath.fromString("s://dd@:12") }
      intercept[MalformedURLException] { ActorPath.fromString("s://dd@h:hd") }
      intercept[MalformedURLException] { ActorPath.fromString("a://l:1/b") }
    }

    "create correct toString" in {
      val a = Address("akka", "mysys")
      RootActorPath(a).toString should ===("akka://mysys/")
      (RootActorPath(a) / "user").toString should ===("akka://mysys/user")
      (RootActorPath(a) / "user" / "foo").toString should ===("akka://mysys/user/foo")
      (RootActorPath(a) / "user" / "foo" / "bar").toString should ===("akka://mysys/user/foo/bar")
    }

    "have correct path elements" in {
      (RootActorPath(Address("akka", "mysys")) / "user" / "foo" / "bar").elements.toSeq should ===(
        Seq("user", "foo", "bar"))
    }

    "create correct toStringWithoutAddress" in {
      val a = Address("akka", "mysys")
      RootActorPath(a).toStringWithoutAddress should ===("/")
      (RootActorPath(a) / "user").toStringWithoutAddress should ===("/user")
      (RootActorPath(a) / "user" / "foo").toStringWithoutAddress should ===("/user/foo")
      (RootActorPath(a) / "user" / "foo" / "bar").toStringWithoutAddress should ===("/user/foo/bar")
    }

    "validate path elements" in {
      intercept[InvalidActorNameException](ActorPath.validatePathElement("")).getMessage should include(
        "must not be empty")
    }

    "create correct toStringWithAddress" in {
      val local = Address("akka", "mysys")
      val a = local.copy(host = Some("aaa"), port = Some(2552))
      val b = a.copy(host = Some("bb"))
      val c = a.copy(host = Some("cccc"))
      val root = RootActorPath(local)
      root.toStringWithAddress(a) should ===("akka://mysys@aaa:2552/")
      (root / "user").toStringWithAddress(a) should ===("akka://mysys@aaa:2552/user")
      (root / "user" / "foo").toStringWithAddress(a) should ===("akka://mysys@aaa:2552/user/foo")

      //      root.toStringWithAddress(b) should ===("akka://mysys@bb:2552/")
      (root / "user").toStringWithAddress(b) should ===("akka://mysys@bb:2552/user")
      (root / "user" / "foo").toStringWithAddress(b) should ===("akka://mysys@bb:2552/user/foo")

      root.toStringWithAddress(c) should ===("akka://mysys@cccc:2552/")
      (root / "user").toStringWithAddress(c) should ===("akka://mysys@cccc:2552/user")
      (root / "user" / "foo").toStringWithAddress(c) should ===("akka://mysys@cccc:2552/user/foo")

      val rootA = RootActorPath(a)
      rootA.toStringWithAddress(b) should ===("akka://mysys@aaa:2552/")
      (rootA / "user").toStringWithAddress(b) should ===("akka://mysys@aaa:2552/user")
      (rootA / "user" / "foo").toStringWithAddress(b) should ===("akka://mysys@aaa:2552/user/foo")
    }

    "not allow path separators in RootActorPath's name" in {
      intercept[IllegalArgumentException] {
        RootActorPath(Address("akka", "mysys"), "/user/boom/*") // illegally pass in a path where name is expected
      }.getMessage should include("is a path separator")

      // check that creating such path still works
      ActorPath.fromString("akka://mysys/user/boom/*")
    }

    "detect valid and invalid chars in host names when not using AddressFromURIString, e.g. docker host given name" in {
      Seq(
        Address("akka", "sys", "valid", 0),
        Address("akka", "sys", "is_valid.org", 0),
        Address("akka", "sys", "fu.is_valid.org", 0)).forall(_.hasInvalidHostCharacters) shouldBe false

      Seq(Address("akka", "sys", "in_valid", 0), Address("akka", "sys", "invalid._org", 0))
        .forall(_.hasInvalidHostCharacters) shouldBe true

      intercept[MalformedURLException](AddressFromURIString("akka://sys@in_valid:5001"))
    }

    "not fail fast if the check is called on valid chars in host names" in {
      Seq(
        Address("akka", "sys", "localhost", 0),
        Address("akka", "sys", "is_valid.org", 0),
        Address("akka", "sys", "fu.is_valid.org", 0)).foreach(_.checkHostCharacters())
    }

    "fail fast if the check is called when invalid chars are in host names" in {
      Seq(
        Address("akka", "sys", "localhost", 0),
        Address("akka", "sys", "is_valid.org", 0),
        Address("akka", "sys", "fu.is_valid.org", 0)).foreach(_.checkHostCharacters())

      intercept[IllegalArgumentException](Address("akka", "sys", "in_valid", 0).checkHostCharacters())
      intercept[IllegalArgumentException](Address("akka", "sys", "invalid._org", 0).checkHostCharacters())
    }
  }
}
