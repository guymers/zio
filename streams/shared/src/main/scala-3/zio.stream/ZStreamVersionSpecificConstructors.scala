package zio.stream

import zio.*
import zio.internal.macros.LayerMacros

import scala.compiletime.constValue
import scala.deriving.Mirror

private[stream] trait ZStreamVersionSpecificConstructors { self: ZStream.type =>
  import ZStreamVersionSpecificConstructors.DistributedWithSumType

  extension[R, E, O](v: ZStream[R, E, O]) {

    inline def distributedWithSumType(maximumLag: => Int)(using
      m: Mirror.SumOf[O],
      trace: Trace
    ): ZIO[R & Scope, Nothing, DistributedWithSumType[E, m.MirroredElemTypes]] = {
      val n = constValue[Tuple.Size[m.MirroredElemTypes]]

      def decide(o: O): UIO[Int => Boolean] = ZIO.succeed(_ == m.ordinal(o))

      v.distributedWith[E](n, maximumLag = maximumLag, decide).map { streams =>
        Tuple
          .fromArray(streams.toArray)
          .asInstanceOf[DistributedWithSumType[E, m.MirroredElemTypes]]
      }
    }
  }
}

object ZStreamVersionSpecificConstructors {

  type DistributedWithSumType[+E, Tup <: Tuple] <: Tuple = Tup match {
    case EmptyTuple => EmptyTuple
    case h *: t => Dequeue[Exit[Option[E], h]] *: DistributedWithSumType[E, t]
  }
}
