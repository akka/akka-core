/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit

import scala.util.control.NoStackTrace

import akka.annotation.{ ApiMayChange, InternalApi }

/**
 * Policies allow to emulate behavior of the storage (failures and rejections).
 *
 * @tparam U type determines operations which storage can perform.
 */
@ApiMayChange
trait ProcessingPolicy[U] {

  /**
   * Emulates behavior of the storage.
   * The function is invoked when any of the plugin's operations is executed.
   * If you need this operation to succeed return [[ProcessingSuccess]],
   * otherwise you should return some of the [[ProcessingFailure]]'s.
   *
   * @param processId persistenceId or other id of the processing operation
   * @param processingUnit details about current operation to be executed
   * @return needed result of processing the operation
   */
  def tryProcess(processId: String, processingUnit: U): ProcessingResult

}

/**
 * INTERNAL API
 */
@InternalApi
object ProcessingPolicy {

  /**
   * INTERNAL API
   */
  @InternalApi
  private[testkit] trait DefaultPolicies[U] {

    type PolicyType = ProcessingPolicy[U]

    case object PassAll extends PolicyType {

      override def tryProcess(persistenceId: String, processingUnit: U): ProcessingResult = ProcessingSuccess

    }

    class RejectNextNCond(
        _numberToFail: Int,
        _failureException: Throwable,
        cond: (String, U) => Boolean,
        onLimitExceed: => Unit = ())
        extends CountNextNCond(_numberToFail, Reject(_failureException), ProcessingSuccess, cond, onLimitExceed)

    class FailNextNCond(
        _numberToFail: Int,
        _failureException: Throwable,
        cond: (String, U) => Boolean,
        onLimitExceed: => Unit = ())
        extends CountNextNCond(_numberToFail, StorageFailure(_failureException), ProcessingSuccess, cond, onLimitExceed)

    class FailNextN(_numberToFail: Int, _failureException: Throwable, onLimitExceed: => Unit = ())
        extends CountNextNCond(
          _numberToFail,
          StorageFailure(_failureException),
          ProcessingSuccess,
          (_, _) => true,
          onLimitExceed)

    class RejectNextN(_numberToReject: Int, _rejectionException: Throwable, onLimitExceed: => Unit = ())
        extends CountNextNCond(
          _numberToReject,
          Reject(_rejectionException),
          ProcessingSuccess,
          (_, _) => true,
          onLimitExceed)

    class ReturnAfterNextNCond(
        returnOnTrigger: => ProcessingResult,
        returnNonTrigger: => ProcessingResult,
        cond: (String, U) => Boolean)
        extends PolicyType {

      override def tryProcess(persistenceId: String, processingUnit: U): ProcessingResult = {
        if (cond(persistenceId, processingUnit)) {
          returnOnTrigger
        } else {
          returnNonTrigger
        }
      }

    }

    class CountNextNCond(
        numberToCount: Int,
        returnOnTrigger: => ProcessingResult,
        returnNonTrigger: => ProcessingResult,
        cond: (String, U) => Boolean,
        onLimitExceed: => Unit)
        extends ReturnAfterNextNCond(returnOnTrigger, returnNonTrigger, new Function2[String, U, Boolean] {

          var counter = 0

          override def apply(persistenceId: String, v1: U): Boolean = {
            val intRes = cond(persistenceId, v1)
            if (intRes && counter < numberToCount) {
              counter += 1
              if (counter == numberToCount) onLimitExceed
              intRes
            } else {
              false
            }
          }
        })

  }

}

/**
 * INTERNAL API
 */
@InternalApi
sealed trait ProcessingResult

sealed abstract class ProcessingSuccess extends ProcessingResult

/**
 * Emulates successful processing of some operation.
 */
case object ProcessingSuccess extends ProcessingSuccess {

  def getInstance(): ProcessingSuccess = this

}

sealed trait ProcessingFailure extends ProcessingResult {

  def error: Throwable

}

sealed abstract class ExpectedRejection extends Throwable

object ExpectedRejection extends ExpectedRejection {

  def getInstance(): ExpectedRejection = this

}

sealed abstract class ExpectedFailure extends Throwable with NoStackTrace

object ExpectedFailure extends ExpectedFailure {

  def getInstance(): ExpectedFailure = this

}

/**
 * Emulates rejection of operation by the journal with `error` exception.
 * Has the same meaning as `StorageFailure` for snapshot storage,
 * because it does not support rejections.
 */
final case class Reject(error: Throwable = ExpectedRejection) extends ProcessingFailure {

  def getError(): Throwable = error

}

object Reject {

  def create(error: Throwable): Reject = Reject(error)

  def create(): Reject = Reject(ExpectedRejection)

}

/**
 * Emulates exception thrown by the storage on the attempt to perform some operation.
 */
final case class StorageFailure(error: Throwable = ExpectedFailure) extends ProcessingFailure {

  def getError(): Throwable = error

}

object StorageFailure {

  def create(error: Throwable): StorageFailure = StorageFailure(error)

  def create(): StorageFailure = StorageFailure(ExpectedFailure)

}
