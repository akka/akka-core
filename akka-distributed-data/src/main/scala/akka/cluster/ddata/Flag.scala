/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

object Flag {

  /**
   * `Flag` that is initialized to `false`.
   */
  val empty: Flag = new Flag(enabled = false)

  /**
   * `Flag` that is initialized to `false`.
   */
  val Disabled: Flag = empty

  /**
   * `Flag` that is initialized to `true`.
   */
  val Enabled: Flag = new Flag(enabled = true)

  def apply(): Flag = Disabled

  /**
   * Java API: `Flag` that is initialized to `false`.
   */
  def create(): Flag = Disabled

  // unapply from case class
}

/**
 * Implements a boolean flag CRDT that is initialized to `false` and
 * can be switched to `true`. `true` wins over `false` in merge.
 *
 * This class is immutable, i.e. "modifying" methods return a new instance.
 */
@SerialVersionUID(1L)
final case class Flag(enabled: Boolean) extends ReplicatedData with ReplicatedDataSerialization {

  type T = Flag

  def switchOn: Flag =
    if (enabled) this
    else Flag.Enabled

  override def merge(that: Flag): Flag =
    if (that.enabled) that
    else this
}

object FlagKey {
  def create(id: String): Key[Flag] = FlagKey(id)
}

@SerialVersionUID(1L)
final case class FlagKey(_id: String) extends Key[Flag](_id) with ReplicatedDataSerialization {
  override def withId(newId: Key.KeyId): FlagKey =
    FlagKey(newId)
}
