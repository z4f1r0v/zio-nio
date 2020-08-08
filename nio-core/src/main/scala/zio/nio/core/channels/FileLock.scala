package zio.nio.core.channels

import java.io.IOException
import java.nio.channels.{ FileLock => JFileLock }
import java.nio.{ channels => jc }

import zio.{ IO, UIO }
import zio.nio.core.IOCloseable

/**
 * A token representing a lock on a region of a file.
 * A file-lock object is created each time a lock is acquired on a file via one of the `lock` or `tryLock` methods
 * of the `FileChannel` class, or the `lock` or `tryLock` methods of the `AsynchronousFileChannel` class.
 */
final class FileLock private[channels] (javaLock: JFileLock) extends IOCloseable[Any] {

  /**
   * The channel upon whose file this lock was acquired.
   * If the underlying NIO channel is a standard channel type, the appropriate ZIO-NIO wrapper class is returned,
   * otherwise a generic [[zio.nio.core.channels.Channel]] is returned.
   */
  def acquiredBy: UIO[Channel] =
    javaLock.acquiredBy() match {
      case c: jc.AsynchronousFileChannel         =>
        UIO.succeed(AsynchronousFileChannel.fromJava(c))
      case c: jc.AsynchronousSocketChannel       =>
        UIO.succeed(AsynchronousSocketChannel.fromJava(c))
      case c: jc.AsynchronousServerSocketChannel =>
        UIO.succeed(AsynchronousServerSocketChannel.fromJava(c))
      case c: jc.DatagramChannel                 =>
        UIO.effectTotal(c.isBlocking).map {
          case true  => DatagramChannel.Blocking.fromJava(c)
          case false => DatagramChannel.NonBlocking.fromJava(c)
        }
      case c: jc.FileChannel                     =>
        UIO.succeed(FileChannel.fromJava(c))
      case c: jc.Pipe.SinkChannel                =>
        UIO.effectTotal(c.isBlocking).map {
          case true  => new Pipe.BlockingSinkChannel(c)
          case false => new Pipe.NonBlockingSinkChannel(c)
        }
      case c: jc.Pipe.SourceChannel              =>
        UIO.effectTotal(c.isBlocking).map {
          case true  => new Pipe.BlockingSourceChannel(c)
          case false => new Pipe.NonBlockingSourceChannel(c)
        }
      case c: jc.SocketChannel                   =>
        UIO.effectTotal(c.isBlocking).map {
          case true  => SocketChannel.Blocking.fromJava(c)
          case false => SocketChannel.NonBlocking.fromJava(c)
        }
      case c: jc.ServerSocketChannel             =>
        UIO.effectTotal(c.isBlocking).map {
          case true  => ServerSocketChannel.Blocking.fromJava(c)
          case false => ServerSocketChannel.NonBlocking.fromJava(c)
        }
      case c                                     =>
        UIO.succeed(new Channel {
          override protected val channel: jc.Channel = c
        })
    }

  /**
   * Returns the position within the file of the first byte of the locked region.
   * A locked region need not be contained within, or even overlap, the actual underlying file,
   * so the value returned by this method may exceed the file's current size.
   */
  def position: Long = javaLock.position

  /**
   * Returns the size of the locked region in bytes.
   * A locked region need not be contained within, or even overlap, the actual underlying file,
   * so the value returned by this method may exceed the file's current size.
   */
  def size: Long = javaLock.size

  /**
   * Tells whether this lock is shared.
   */
  def isShared: Boolean = javaLock.isShared

  /**
   * Tells whether or not this lock overlaps the given lock range.
   *
   * @param position The starting position of the lock range
   * @param size The size of the lock range
   */
  def overlaps(position: Long, size: Long): Boolean = javaLock.overlaps(position, size)

  /**
   * Tells whether or not this lock is valid.
   * A lock object remains valid until it is released or the associated file channel is closed, whichever comes first.
   */
  def isValid: UIO[Boolean] = UIO.effectTotal(javaLock.isValid)

  /**
   * Releases this lock.
   *
   * If this lock object is valid then invoking this method releases the lock and renders the object invalid.
   * If this lock object is invalid then invoking this method has no effect.
   */
  def release: IO[IOException, Unit] = IO.effect(javaLock.release()).refineToOrDie[IOException]

  /**
   * Closes this file lock.
   *
   * Alias for `release`.
   */
  override def close: IO[IOException, Unit] = release
}

object FileLock {
  def fromJava(javaLock: JFileLock): FileLock = new FileLock(javaLock)
}
