/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import org.agrona.concurrent.ManyToManyConcurrentArrayQueue

/**
 * INTERNAL API
 */
private[remote] class ObjectPool[A <: AnyRef](capacity: Int, create: () => A, clear: A => Unit) {
  private val pool = new ManyToManyConcurrentArrayQueue[A](capacity)

  def acquire(): A = {
    val obj = pool.poll()
    if (obj eq null) create()
    else obj
  }

  def release(obj: A): Boolean = {
    clear(obj)
    (!pool.offer(obj))
  }
}
