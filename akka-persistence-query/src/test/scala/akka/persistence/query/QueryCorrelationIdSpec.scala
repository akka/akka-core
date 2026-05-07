/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query

import akka.testkit.TestException
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.UUID

class QueryCorrelationIdSpec extends AnyWordSpecLike with Matchers {

  def pretendQueryMethod(): Option[String] =
    QueryCorrelationId.get()

  "The query correlation id utility" should {

    "pass and clear correlation id" in {
      val uuid = UUID.randomUUID().toString
      val observed =
        QueryCorrelationId.withCorrelationId(uuid) { () =>
          pretendQueryMethod()
        }
      observed shouldEqual Some(uuid)

      // cleared after returning
      QueryCorrelationId.get() shouldBe None
    }

    "pass along and clear correlation id if present" in {
      val uuid = UUID.randomUUID().toString
      val observed =
        QueryCorrelationId.withCorrelationId(Some(uuid)) { () =>
          pretendQueryMethod()
        }
      observed shouldEqual Some(uuid)

      // cleared after returning
      QueryCorrelationId.get() shouldBe None
    }

    "just invoke the block if correlation id not present" in {
      val observed =
        QueryCorrelationId.withCorrelationId(None) { () =>
          pretendQueryMethod()
        }
      observed shouldEqual None
    }

    "clear correlation id when call fails" in {
      val uuid = UUID.randomUUID().toString
      intercept[TestException] {
        QueryCorrelationId.withCorrelationId(uuid) { () =>
          throw TestException("boom")
        }
      }

      // cleared after throwing
      QueryCorrelationId.get() shouldBe None
    }

  }

}
