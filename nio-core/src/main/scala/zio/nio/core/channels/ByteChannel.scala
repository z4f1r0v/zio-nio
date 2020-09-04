package zio.nio.core.channels

import zio.ZIO
import zio.blocking.Blocking
import java.nio.channels.{
  GatheringByteChannel => JGatheringByteChannel,
  ScatteringByteChannel => JScatteringByteChannel
}

/**
 * A channel that can both read and write bytes.
 */
trait ByteChannel extends GatheringByteChannel with ScatteringByteChannel {

  override protected[channels] val channel: JGatheringByteChannel with JScatteringByteChannel

}

trait BlockingByteChannel
    extends ByteChannel
    with GatheringByteChannel.Blocking
    with ScatteringByteChannel.Blocking
    with WithEnv.Blocking {

  self =>

  import BlockingByteChannel._

  /**
   * Efficiently performs a set of interruptible blocking read and/or write operations.
   *
   * Performing blocking I/O in a ZIO effect with support for effect interruption imposes some overheads.
   * Ideally, all the reads/writes with this channel should be done in an effect value passed to this method,
   * so that the blocking overhead cost is paid only once.
   *
   * @param f And effect that can perform reads/writes given a non-blocking byte channel.
   *          The entire effect is run on the same thread in the blocking pool.
   */
  def blocking[R, E, A](f: ScopedBlockingReadsAndWrites => ZIO[R, E, A]): ZIO[R with Blocking, E, A] = {
    val channel = new ScopedBlockingReadsAndWrites {
      override protected[channels] val channel = self.channel
    }
    WithEnv.withBlocking(channel)(f(channel))
  }

}

object BlockingByteChannel {

  trait ScopedBlockingReadsAndWrites
      extends ByteChannel
      with GatheringByteChannel.SinkWrite
      with ScatteringByteChannel.StreamRead
      with WithEnv.NonBlocking

}
