package zio.stream

import zio.ZIO
import zio.Scope
import zio.Exit
import zio.Trace
import scala.compiletime.constValue
import scala.deriving.Mirror

private[stream] trait ZStreamVersionSpecificConstructors { self: ZStream.type =>

  extension [R, E, A](stream: ZStream[R, E, A]) {

    /**
     * Distributes a stream into one for each of its sub types.
     */
    inline def distributedSumType(maximumLag: => Int)(using
      m: Mirror.SumOf[A],
      trace: Trace
    ): ZIO[R & Scope, Nothing, Tuple.Map[m.MirroredElemTypes, [a] =>> ZStream[Any, E, a]]] = {
      val n = constValue[Tuple.Size[m.MirroredElemTypes]]

      def decide(a: A): ZIO[Any, Nothing, Int => Boolean] = Exit.succeed(_ == m.ordinal(a))

      stream.distributedWith(n, maximumLag = maximumLag, decide).map { ls =>
        val streams = ls.map(ZStream.fromQueueWithShutdown(_).flattenExitOption)
        Tuple.fromArray(streams.toArray).asInstanceOf[Tuple.Map[m.MirroredElemTypes, [a] =>> ZStream[Any, E, a]]]
      }
    }
  }
}
