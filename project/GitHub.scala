/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

object GitHub {

  def envTokenOrThrow: Option[String] =
    sys.env.get("PR_VALIDATOR_GH_TOKEN").orElse {
      if (sys.env.contains("ghprbPullId")) {
        throw new Exception(
          "No PR_VALIDATOR_GH_TOKEN env var provided during GitHub Pull Request Builder build, unable to reach GitHub!")
      } else {
        None
      }
    }

  def url(v: String): String = {
    val branch = if (v.endsWith("SNAPSHOT")) "main" else "v" + v
    "https://github.com/akka/akka/tree/" + branch
  }
}
