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
