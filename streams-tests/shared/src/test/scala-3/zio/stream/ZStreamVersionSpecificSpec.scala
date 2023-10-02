package zio.stream

import zio.*
import zio.test.*

object ZStreamVersionSpecificSpec extends ZIOBaseSpec {

  def spec =
    suite("ZStreamVersionSpecificSpec")(
      suite("Combinators")(
        test("broadcastN") {
          ZIO.scoped {
            ZStream
              .range(0, 5)
              .broadcastN(2, 16)
              .flatMap { case (s1, s2) =>
                for {
                  out1    <- s1.runCollect
                  out2    <- s2.runCollect
                  expected = Chunk.fromIterable(Range(0, 5))
                } yield assertTrue(out1 == expected) && assertTrue(out2 == expected)
              }
          }
        },
        test("distributedWithSumType") {
          ZIO.scoped {
            val stream = ZStream(Animal.Cat("c1"), Animal.Dog("d1"), Animal.Cat("c2"))
            stream.distributedWithSumType(16).flatMap { case (catStream, dogStream) =>
              for {
                cats <- ZStream.fromQueue(catStream).flattenExitOption.runCollect
                dogs <- ZStream.fromQueue(dogStream).flattenExitOption.runCollect
              } yield {
                assertTrue(cats == Chunk(Animal.Cat("c1"), Animal.Cat("c2"))) &&
                  assertTrue(cats.map(_.meow) == Chunk("meow c1", "meow c2")) &&
                  assertTrue(dogs == Chunk(Animal.Dog("d1"))) &&
                  assertTrue(dogs.map(_.woof) == Chunk("woof d1"))
              }
            }
          }
        }
      ),
    )

  sealed trait Animal
  object Animal {
    case class Cat(name: String) extends Animal {
      def meow = s"meow $name"
    }
    case class Dog(name: String) extends Animal {
      def woof = s"woof $name"
    }
  }
}
