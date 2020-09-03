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
trait ByteChannel[R] extends GatheringByteChannel[R] with ScatteringByteChannel[R] {

  override protected[channels] val channel: JGatheringByteChannel with JScatteringByteChannel

}

trait BlockingByteChannel
    extends ByteChannel[Blocking]
    with GatheringByteChannel.Blocking
    with ScatteringByteChannel.Blocking {

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
  def blocking[R, E, A](f: Streamable => ZIO[R, E, A]): ZIO[R with Blocking, E, A] = {
    val channel = new ByteChannel[Any]
      with GatheringByteChannel.SinkWrite[Any]
      with ScatteringByteChannel.StreamRead[Any]
      with WithEnv.NonBlocking {
      override protected[channels] val channel = self.channel
    }
    WithEnv.withBlocking(channel)(f(channel))
  }

}

object BlockingByteChannel {

  type Streamable = ByteChannel[Any] with GatheringByteChannel.SinkWrite[Any] with ScatteringByteChannel.StreamRead[Any]

}
