/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import akka.testkit.AkkaSpec

class ManifestInfoSpec extends AkkaSpec {
  "ManifestInfo" should {
    "produce a clear message" in {
      val versions = Map(
        "akka-actor" -> new ManifestInfo.Version("2.6.4"),
        "akka-persistence" -> new ManifestInfo.Version("2.5.3"),
        "akka-cluster" -> new ManifestInfo.Version("2.5.3"),
        "unrelated" -> new ManifestInfo.Version("2.5.3"))
      val allModules = List("akka-actor", "akka-persistence", "akka-cluster")
      ManifestInfo.checkSameVersion("Akka", allModules, versions) shouldBe Some(
        "You are using version 2.6.4 of Akka, but it appears you (perhaps indirectly) also depend on older versions of related artifacts. " +
        "You can solve this by adding an explicit dependency on version 2.6.4 of the [akka-persistence, akka-cluster] artifacts to your project. " +
        "Here's a complete collection of detected artifacts: (2.5.3, [akka-cluster, akka-persistence]), (2.6.4, [akka-actor]). " +
        "See also: https://doc.akka.io/libraries/akka-core/current/common/binary-compatibility-rules.html#mixed-versioning-is-not-allowed")
    }

    "support dynver" in {
      val versions = Map(
        "akka-actor" -> new ManifestInfo.Version("2.6.4"),
        "akka-persistence" -> new ManifestInfo.Version("2.6.4+10-abababef"))
      val allModules = List("akka-actor", "akka-persistence")
      ManifestInfo.checkSameVersion("Akka", allModules, versions) shouldBe Some(
        "You are using version 2.6.4+10-abababef of Akka, but it appears you (perhaps indirectly) also depend on older versions of related artifacts. " +
        "You can solve this by adding an explicit dependency on version 2.6.4+10-abababef of the [akka-actor] artifacts to your project. " +
        "Here's a complete collection of detected artifacts: (2.6.4, [akka-actor]), (2.6.4+10-abababef, [akka-persistence]). " +
        "See also: https://doc.akka.io/libraries/akka-core/current/common/binary-compatibility-rules.html#mixed-versioning-is-not-allowed")
    }
  }
}
