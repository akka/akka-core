/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.serialization

/**
 * Use as replacement for an original exception when it can't be serialized or deserialized.
 * @param originalMessage the message of the original exception
 * @param originalClassName the class name of the original exception
 * @param cause exception that caused deserialization error, optional and will not be serialized
 */
final class ThrowableNotSerializableException(
    val originalMessage: String,
    val originalClassName: String,
    cause: Throwable)
    extends IllegalArgumentException(s"Serialization of [$originalClassName] failed. $originalMessage", cause) {

  def this(originalMessage: String, originalClassName: String) = this(originalMessage, originalClassName, null)
}
