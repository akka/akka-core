/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object TypedMultiMapSpec {
  trait AbstractKey { type Type }
  final case class Key[T](t: T) extends AbstractKey { final override type Type = T }
  final case class MyValue[T](t: T)

  type KV[K <: AbstractKey] = MyValue[K#Type]
}

class TypedMultiMapSpec extends AnyWordSpec with Matchers with TypeCheckedTripleEquals {
  import TypedMultiMapSpec._

  "A TypedMultiMap" must {

    "retain and remove values for the same key" in {
      val m1 = TypedMultiMap.empty[AbstractKey, KV]
      val m2 = m1.inserted(Key(1))(MyValue(42))
      m2.get(Key(1)) should ===(Set(MyValue(42)))
      m2.removed(Key(1))(MyValue(42)).get(Key(1)) should ===(Set.empty[MyValue[Int]])
      val m3 = m2.inserted(Key(1))(MyValue(43))
      m3.get(Key(1)) should ===(Set(MyValue(42), MyValue(43)))
      m3.removed(Key(1))(MyValue(42)).get(Key(1)) should ===(Set(MyValue(43)))
    }

    "retain and remove values for multiple keys" in {
      val m1 = TypedMultiMap.empty[AbstractKey, KV]
      val m2 = m1.inserted(Key(1))(MyValue(42)).inserted(Key(2))(MyValue(43))
      m2.get(Key(1)) should ===(Set(MyValue(42)))
      m2.removed(Key(1))(MyValue(42)).get(Key(1)) should ===(Set.empty[MyValue[Int]])
      m2.get(Key(2)) should ===(Set(MyValue(43)))
      m2.removed(Key(1))(MyValue(42)).get(Key(2)) should ===(Set(MyValue(43)))
    }

    "remove a value from all keys" in {
      val m1 = TypedMultiMap.empty[AbstractKey, KV]
      val m2 = m1.inserted(Key(1))(MyValue(42)).inserted(Key(2))(MyValue(43)).inserted(Key(2))(MyValue(42))
      val m3 = m2.valueRemoved(MyValue(42))
      m3.get(Key(1)) should ===(Set.empty[MyValue[Int]])
      m3.get(Key(2)) should ===(Set(MyValue(43)))
      m3.keySet should ===(Set[AbstractKey](Key(2)))
    }

    "remove all values from a key" in {
      val m1 = TypedMultiMap.empty[AbstractKey, KV]
      val m2 = m1.inserted(Key(1))(MyValue(42)).inserted(Key(2))(MyValue(43)).inserted(Key(2))(MyValue(42))
      val m3 = m2.keyRemoved(Key(1))
      m3.get(Key(1)) should ===(Set.empty[MyValue[Int]])
      m3.get(Key(2)) should ===(Set(MyValue(42), MyValue(43)))
      m3.keySet should ===(Set[AbstractKey](Key(2)))
    }

    "reject invalid insertions" in {
      "TypedMultiMap.empty[AbstractKey, KV].inserted(Key(1))(MyValue(42L))" shouldNot compile
    }

    "reject invalid removals" in {
      "TypedMultiMap.empty[AbstractKey, KV].removed(Key(1))(MyValue(42L))" shouldNot compile
    }

  }

}
