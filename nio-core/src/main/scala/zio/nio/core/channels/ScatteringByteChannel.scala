package zio.nio.core

package channels

import java.io.{ EOFException, IOException }
import java.nio.{ ByteBuffer => JByteBuffer }
import java.nio.channels.{ ScatteringByteChannel => JScatteringByteChannel }

import zio._
import zio.stream.ZStream

/**
 * A channel that can read bytes into a sequence of buffers.
 */
trait ScatteringByteChannel[R] extends Channel with WithEnv[R] {

  import ScatteringByteChannel._

  override protected[channels] val channel: JScatteringByteChannel

  /**
   * Reads a sequence of bytes from this channel into the provided list of buffers, in order.
   *
   * Fails with `java.io.EOFException` if end-of-stream is reached.
   *
   * @return The number of bytes read in total, possibly 0
   */
  final def read(dsts: Seq[ByteBuffer]): ZIO[R, IOException, Long] =
    withEnv {
      IO.effect(channel.read(unwrap(dsts))).refineToOrDie[IOException].flatMap(eofCheck)
    }

  /**
   * Reads a sequence of bytes from this channel into the given buffer.
   *
   * Fails with `java.io.EOFException` if end-of-stream is reached.
   *
   * @return The number of bytes read, possibly 0
   */
  final def read(dst: ByteBuffer): ZIO[R, IOException, Int] =
    withEnv {
      IO.effect(channel.read(dst.byteBuffer)).refineToOrDie[IOException].flatMap(eofCheck)
    }

  /**
   * Reads a chunk of bytes.
   *
   * Fails with `java.io.EOFException` if end-of-stream is reached.
   *
   * @param capacity The maximum number of bytes to be read.
   * @return The bytes read, between 0 and `capacity` in size, inclusive
   */
  final def readChunk(capacity: Int): ZIO[R, IOException, Chunk[Byte]] =
    for {
      buffer <- Buffer.byte(capacity)
      _      <- read(buffer)
      _      <- buffer.flip
      chunk  <- buffer.getChunk()
    } yield chunk

  /**
   * Reads a sequence of bytes grouped into multiple chunks.
   *
   * Fails with `java.io.EOFException` if end-of-stream is reached.
   *
   * @param capacities For each int in this sequence, a chunk of that size is produced, if there is enough data in the channel.
   * @return A list with one `Chunk` per input size. Some chunks may be less than the requested size if the channel
   *         does not have enough data
   */
  final def readChunks(capacities: Seq[Int]): ZIO[R, IOException, List[Chunk[Byte]]] =
    for {
      buffers <- IO.foreach(capacities)(Buffer.byte)
      _       <- read(buffers)
      chunks  <- IO.foreach(buffers.init)(buf => buf.flip *> buf.getChunk())
    } yield chunks.toList

}

object ScatteringByteChannel {

  private def unwrap(dsts: Seq[ByteBuffer]): Array[JByteBuffer] =
    dsts.map(d => d.byteBuffer).toArray

  trait Blocking extends ScatteringByteChannel[blocking.Blocking] with WithEnv.Blocking {

    /**
     * A `ZStream` that reads from this channel.
     *
     * The stream terminates without error if the channel reaches end-of-stream.
     *
     * @param bufferConstruct Optional, overrides how to construct the buffer used to transfer bytes read from this channel into the stream.
     */
    def stream(
      bufferConstruct: URIO[blocking.Blocking, ByteBuffer] = Buffer.byte(5000)
    ): ZStream[blocking.Blocking, IOException, Byte] =
      ZStream {
        bufferConstruct.toManaged_.map { buffer =>
          val doRead = for {
            _     <- read(buffer)
            _     <- buffer.flip
            chunk <- buffer.getChunk()
            _     <- buffer.clear
          } yield chunk
          doRead.mapError {
            case _: EOFException => None
            case e               => Some(e)
          }
        }
      }

  }

}
