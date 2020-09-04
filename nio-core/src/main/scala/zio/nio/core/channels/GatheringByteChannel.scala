package zio.nio.core.channels

import java.io.IOException
import java.nio.{ ByteBuffer => JByteBuffer }
import java.nio.channels.{ GatheringByteChannel => JGatheringByteChannel }

import zio._
import zio.nio.core.{ Buffer, ByteBuffer }
import zio.stream.ZSink

/**
 * A channel that can write bytes from a sequence of buffers.
 */
trait GatheringByteChannel extends Channel with WithEnv {

  import GatheringByteChannel._

  override protected[channels] val channel: JGatheringByteChannel

  final def write(srcs: List[ByteBuffer]): ZIO[Env, IOException, Long] =
    withEnv {
      IO.effect(channel.write(unwrap(srcs))).refineToOrDie[IOException]
    }

  final def write(src: ByteBuffer): ZIO[Env, IOException, Int] =
    withEnv {
      IO.effect(channel.write(src.byteBuffer)).refineToOrDie[IOException]
    }

  /**
   * Writes a list of chunks, in order.
   *
   * Multiple writes may be performed in order to write all the chunks.
   */
  final def writeChunks(srcs: List[Chunk[Byte]]): ZIO[Env, IOException, Unit] =
    for {
      bs <- IO.foreach(srcs)(Buffer.byte)
      _  <- {
        // Handle partial writes by dropping buffers where `hasRemaining` returns false,
        // meaning they've been completely written
        def go(buffers: List[ByteBuffer]): ZIO[Env, IOException, Unit] =
          write(buffers) *>
            IO.foreach(buffers)(b => b.hasRemaining.map(_ -> b)).flatMap { pairs =>
              val remaining = pairs.dropWhile(!_._1).map(_._2)
              if (remaining.isEmpty) IO.unit else go(remaining)
            }
        go(bs)
      }
    } yield ()

  /**
   * Writes a chunk of bytes.
   *
   * Multiple writes may be performed to write the entire chunk.
   */
  final def writeChunk(src: Chunk[Byte]): ZIO[Env, IOException, Unit] = writeChunks(List(src))

}

object GatheringByteChannel {

  private def unwrap(srcs: List[ByteBuffer]): Array[JByteBuffer] =
    srcs.map(d => d.byteBuffer).toArray

  trait SinkWrite {

    this: GatheringByteChannel =>

    /**
     * A sink that will write all the bytes it receives to this channel.
     *
     * @param bufferConstruct Optional, overrides how to construct the buffer used to transfer bytes received by the sink to this channel.
     */
    def sink(
      bufferConstruct: URIO[Env, ByteBuffer] = Buffer.byte(5000)
    ): ZSink[Env, IOException, Byte, Nothing, Long] =
      ZSink {
        for {
          buffer   <- bufferConstruct.toManaged_
          countRef <- Ref.makeManaged(0L)
        } yield (_: Option[Chunk[Byte]])
          .map { chunk =>
            val doWrite = for {
              _     <- buffer.putChunk(chunk)
              _     <- buffer.flip
              count <- write(buffer)
              _     <- ZIO.whenM(buffer.hasRemaining)(ZIO.dieMessage("Blocking channel did not write the entire buffer"))
              _     <- buffer.clear
            } yield count
            doWrite.foldM(
              e => ZIO.fail((Left(e), Chunk.empty)),
              count => countRef.update(_ + count.toLong)
            )
          }
          .getOrElse(
            countRef.get.flatMap[Any, (Either[IOException, Long], Chunk[Nothing]), Unit](count =>
              IO.fail((Right(count), Chunk.empty))
            )
          )
      }

  }

  trait ScopedBlockingWrites extends GatheringByteChannel with SinkWrite with WithEnv.NonBlocking

  trait Blocking extends GatheringByteChannel with SinkWrite with WithEnv.Blocking {

    self =>

    /**
     * Efficiently performs a set of interruptible blocking write operations.
     *
     * Performing blocking I/O in a ZIO effect with support for effect interruption imposes some overheads.
     * Ideally, all the writes to this channel should be done in an effect value passed to this method,
     * so that the blocking overhead cost is paid only once.
     *
     * @param f And effect that can perform writes given a non-blocking writable byte channel.
     *          The entire effect is run on the same thread in the blocking pool.
     */
    def blockingWrites[R, E, A](
      f: ScopedBlockingWrites => ZIO[R, E, A]
    ): ZIO[R with blocking.Blocking, E, A] = {
      val channel = new ScopedBlockingWrites {
        override protected[channels] val channel = self.channel
      }
      WithEnv.withBlocking(channel)(f(channel))
    }
  }
}
