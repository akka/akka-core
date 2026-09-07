/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.tcp.ssl

private[ssl] object TestResources {
  def nameToPath(name: String): String = getClass.getClassLoader.getResource(name).getPath
}
