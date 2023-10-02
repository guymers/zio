package zio.stream

import zio.*
import zio.internal.macros.LayerMacros

import scala.compiletime.constValue
import scala.deriving.Mirror

trait ZStreamVersionSpecific[-R, +E, +O] { self: ZStream[R, E, O] =>
  import ZStreamVersionSpecific.BroadcastN

  /**
   * Fan out the stream, producing `n` streams that have the same elements
   * as this stream. The driver stream will only ever advance the `maximumLag`
   * chunks before the slowest downstream stream.
   */
  def broadcastN[N <: Int](n: N, maximumLag: => Int)(using
    trace: Trace
  ): ZIO[R & Scope, Nothing, BroadcastN[n.type, E, O]] = {
    self.broadcast(n, maximumLag = maximumLag).map { streams =>
      Tuple
        .fromArray(streams.toArray)
        .asInstanceOf[BroadcastN[n.type, E, O]]
    }
  }

  /**
   * Automatically assembles a layer for the ZStream effect,
   * which translates it to another level.
   */
  inline def provide[E1 >: E](inline layer: ZLayer[_,E1,_]*): ZStream[Any, E1, O] =
    ${ZStreamProvideMacro.provideImpl[Any, R, E1, O]('self, 'layer)}

}

object ZStreamVersionSpecific {
  import scala.compiletime.ops.int.S

  type BroadcastN[N <: Int, +E, +O] <: Tuple = N match {
    case 2 => ZStream[Any, E, O] *: ZStream[Any, E, O] *: EmptyTuple
    case S[n] => ZStream[Any, E, O] *: BroadcastN[n, E, O]
  }
}

object ZStreamProvideMacro {
  import scala.quoted._

  def provideImpl[R0: Type, R: Type, E: Type, A: Type](zstream: Expr[ZStream[R,E,A]], layer: Expr[Seq[ZLayer[_,E,_]]])(using Quotes): Expr[ZStream[R0,E,A]] = {
    val layerExpr = LayerMacros.constructLayer[R0, R, E](layer)
    '{$zstream.provideLayer($layerExpr.asInstanceOf[ZLayer[R0,E,R]])}
  }
}
