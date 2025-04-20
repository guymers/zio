package zio.stream

import zio.*
import zio.internal.macros.LayerMacros
import scala.deriving.Mirror
import scala.compiletime.ops.int.*

private[stream] trait ZStreamVersionSpecific[-R, +E, +O] { self: ZStream[R, E, O] =>
  import ZStreamVersionSpecific.RepeatN

  /**
   * Fan out the stream, producing a tuple of `n` streams that have the same
   * elements as this stream. The driver stream will only ever advance the
   * `maximumLag` chunks before the slowest downstream stream.
   */
  def broadcastN[N <: Int](n: N, maximumLag: => Int)(using
    n.type >= 2 =:= true,
    Trace
  ): ZIO[R & Scope, Nothing, RepeatN[n.type, ZStream[Any, E, O]]] =
    broadcast(n, maximumLag = maximumLag).map { chunk =>
      Tuple.fromArray(chunk.toArray).asInstanceOf[RepeatN[n.type, ZStream[Any, E, O]]]
    }

  /**
   * Converts the stream to a tuple of `n` queues. Every value will be
   * replicated to every queue with the slowest queue being allowed to buffer
   * `maximumLag` chunks before the driver is back pressured.
   *
   * Queues can unsubscribe from upstream by shutting down.
   */
  def broadcastedQueuesN[N <: Int](n: N, maximumLag: => Int)(using
    n.type >= 2 =:= true,
    Trace
  ): ZIO[R & Scope, Nothing, RepeatN[n.type, Dequeue[Take[E, O]]]] =
    broadcastedQueues(n, maximumLag = maximumLag).map { chunk =>
      Tuple.fromArray(chunk.toArray).asInstanceOf[RepeatN[n.type, Dequeue[Take[E, O]]]]
    }

  /**
   * More powerful version of `ZStream#broadcast`. Allows to provide a function
   * that determines what queues should receive which elements. The decide
   * function will receive the indices of the queues in the resulting tuple.
   */
  def distributedWithN[N <: Int](n: N, maximumLag: => Int, decide: O => UIO[Int => Boolean])(using
    n.type >= 2 =:= true,
    Trace
  ): ZIO[R & Scope, Nothing, RepeatN[n.type, Dequeue[Exit[Option[E], O]]]] =
    distributedWith(n, maximumLag = maximumLag, decide).map { ls =>
      Tuple.fromArray(ls.toArray).asInstanceOf[RepeatN[n.type, Dequeue[Exit[Option[E], O]]]]
    }

  /**
   * Automatically assembles a layer for the ZStream effect, which translates it
   * to another level.
   */
  inline def provide[E1 >: E](inline layer: ZLayer[_, E1, _]*): ZStream[Any, E1, O] =
    ${ ZStreamProvideMacro.provideImpl[Any, R, E1, O]('self, 'layer) }

}

private[stream] object ZStreamProvideMacro {
  import scala.quoted._

  def provideImpl[R0: Type, R: Type, E: Type, A: Type](
    zstream: Expr[ZStream[R, E, A]],
    layer: Expr[Seq[ZLayer[_, E, _]]]
  )(using Quotes): Expr[ZStream[R0, E, A]] = {
    val layerExpr = LayerMacros.constructStaticLayer[R0, R, E](layer)
    '{ $zstream.provideLayer($layerExpr.asInstanceOf[ZLayer[R0, E, R]]) }
  }
}

private[stream] object ZStreamVersionSpecific {

  type RepeatN[N <: Int, +A] <: Tuple = N match {
    case 0    => EmptyTuple
    case S[n] => A *: RepeatN[n, A]
  }
}
