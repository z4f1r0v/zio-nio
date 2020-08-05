package zio.nio.core.channels

import java.io.IOException
import java.nio.channels.{ Pipe => JPipe }

import zio.{ IO, blocking }

final class Pipe private (private val pipe: JPipe) {

  /**
   * Returns this pipe's source channel in blocking mode.
   * Note that this places the underlying NIO channel in blocking mode, so this method should not be used in
   * conjunction with `sourceNonBlocking`.
   */
  def sourceBlocking: IO[IOException, Pipe.BlockingSourceChannel] =
    IO.effect {
      val source = pipe.source()
      source.configureBlocking(true)
      new Pipe.BlockingSourceChannel(source)
    }.refineToOrDie[IOException]

  /**
   * Returns this pipe's sink channel in blocking mode.
   * Note that this places the underlying NIO channel in blocking mode, so this method should not be used in
   * conjunction with `sinkNonBlocking`.
   */
  def sinkBlocking: IO[IOException, Pipe.BlockingSinkChannel] =
    IO.effect {
      val sink = pipe.sink()
      sink.configureBlocking(true)
      new Pipe.BlockingSinkChannel(sink)
    }.refineToOrDie[IOException]

  /**
   * Returns this pipe's source channel in non-blocking mode.
   * Note that this places the underlying NIO channel in non-blocking mode, so this method should not be used in
   * conjunction with `sourceBlocking`.
   */
  def sourceNonBlocking: IO[IOException, Pipe.NonBlockingSourceChannel] =
    IO.effect {
      val source = pipe.source()
      source.configureBlocking(false)
      new Pipe.NonBlockingSourceChannel(source)
    }.refineToOrDie[IOException]

  /**
   * Returns this pipe's sink channel in non-blocking mode.
   * Note that this places the underlying NIO channel in non-blocking mode, so this method should not be used in
   * conjunction with `sinkBlocking`.
   */
  def sinkNonBlocking: IO[IOException, Pipe.NonBlockingSinkChannel] =
    IO.effect {
      val sink = pipe.sink()
      sink.configureBlocking(false)
      new Pipe.NonBlockingSinkChannel(sink)
    }.refineToOrDie[IOException]

}

object Pipe {

  sealed abstract class SinkChannel[R](override protected[channels] val channel: JPipe.SinkChannel)
      extends ModalChannel
      with GatheringByteChannel[R]

  sealed abstract class SourceChannel[R](override protected[channels] val channel: JPipe.SourceChannel)
      extends ModalChannel
      with ScatteringByteChannel[R]

  final class BlockingSinkChannel(c: JPipe.SinkChannel)
      extends SinkChannel[blocking.Blocking](c)
      with GatheringByteChannel.Blocking {}

  final class BlockingSourceChannel(c: JPipe.SourceChannel)
      extends SourceChannel[blocking.Blocking](c)
      with ScatteringByteChannel.Blocking {}

  final class NonBlockingSinkChannel(c: JPipe.SinkChannel) extends SinkChannel[Any](c) with SelectableChannel {}

  final class NonBlockingSourceChannel(c: JPipe.SourceChannel) extends SourceChannel[Any](c) with SelectableChannel {}

  val open: IO[IOException, Pipe] =
    IO.effect(new Pipe(JPipe.open())).refineToOrDie[IOException]

  def fromJava(javaPipe: JPipe): Pipe = new Pipe(javaPipe)

}
