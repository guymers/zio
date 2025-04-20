package zio.stream

import zio.ZIO
import zio.Chunk
import zio.ZIOBaseSpec
import zio.test.Assertion.*
import zio.test.*
import scala.deriving.Mirror

object ZStreamVersionSpecificSpec extends ZIOBaseSpec {

  def spec = suite("ZStreamSpec")(
    test("broadcastN") {
      ZIO.scoped {
        val stream = ZStream.fromChunk(Chunk(Animal.Cat("cat"), Animal.Dog("dog"), Animal.Frog("frog")))
        stream.broadcastN(3, 16).flatMap { case (s1, s2, s3) =>
          ZStream
            .mergeAllUnbounded()(
              s1.map(c => (1, Animal.name(c))),
              s2.map(d => (2, Animal.name(d))),
              s3.map(f => (3, Animal.name(f)))
            )
            .runCollect
        }
      }.map { result =>
        assertTrue(result.sorted == Chunk(1, 2, 3).flatMap(i => Chunk("cat", "dog", "frog").map((i, _))))
      }
    },
    test("distributedSumType") {
      ZIO.scoped {
        val stream = ZStream.fromChunk(Chunk(Animal.Cat("cat"), Animal.Dog("dog"), Animal.Frog("frog")))
        stream.distributedSumType(16).flatMap { case (cats, dogs, frogs) =>
          ZStream
            .mergeAllUnbounded()(
              cats.map(c => (1, Animal.name(c))),
              dogs.map(d => (2, Animal.name(d))),
              frogs.map(f => (3, Animal.name(f)))
            )
            .runCollect
        }
      }.map { result =>
        assertTrue(result.sorted == Chunk((1, "cat"), (2, "dog"), (3, "frog")))
      }
    },
    test("distributedWithN") {
      ZIO.scoped {
        val m      = summon[Mirror.SumOf[Animal]]
        val stream = ZStream.fromChunk(Chunk(Animal.Cat("cat"), Animal.Dog("dog"), Animal.Frog("frog")))
        stream.distributedWithN(3, 16, a => ZIO.succeed(_ == m.ordinal(a))).flatMap { case (cats, dogs, frogs) =>
          ZStream
            .mergeAllUnbounded()(
              ZStream.fromQueueWithShutdown(cats).flattenExitOption.map(c => (1, Animal.name(c))),
              ZStream.fromQueueWithShutdown(dogs).flattenExitOption.map(d => (2, Animal.name(d))),
              ZStream.fromQueueWithShutdown(frogs).flattenExitOption.map(f => (3, Animal.name(f)))
            )
            .runCollect
        }
      }.map { result =>
        assertTrue(result.sorted == Chunk((1, "cat"), (2, "dog"), (3, "frog")))
      }
    }
  )

  sealed trait Animal
  object Animal {
    case class Cat(name: String) extends Animal
    case class Dog(name: String) extends Animal
    case class Frog(n: String)   extends Animal

    def name(a: Animal) = a match {
      case Cat(name)  => name
      case Dog(name)  => name
      case Frog(name) => name
    }
  }
}
