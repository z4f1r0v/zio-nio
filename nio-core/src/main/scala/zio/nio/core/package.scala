package zio.nio

import java.io.{ EOFException, IOException }

import zio._
import zio.ZManaged.ReleaseMap

package object core {

  /**
   * Handle -1 magic number returned by many Java read APIs when end of file is reached.
   *
   * Produces an `EOFException` failure if `value` < 0, otherwise succeeds with `value`.
   */
  private[nio] def eofCheck(value: Int): IO[EOFException, Int] =
    if (value < 0) IO.fail(new EOFException("Channel has reached the end of stream")) else IO.succeed(value)

  /**
   * Handle -1 magic number returned by many Java read APIs when end of file is reached.
   *
   * Produces an `EOFException` failure if `value` < 0, otherwise succeeds with `value`.
   */
  private[nio] def eofCheck(value: Long): IO[EOFException, Long] =
    if (value < 0L) IO.fail(new EOFException("Channel has reached the end of stream")) else IO.succeed(value)

  /**
   * Turns `EOFException` failures into a success with no result.
   */
  def eofOption[R, A, E <: Throwable](effect: ZIO[R, E, A]): ZIO[R, E, Option[A]] =
    effect.asSome.catchSome {
      case _: EOFException => ZIO.none
    }

  implicit final class CloseableResourceOps[-R, +E, +A <: IOCloseable[R]](val value: ZIO[R, E, A]) extends AnyVal {

    /**
     * Lifts an effect producing a closeable NIO resource into a `ZManaged` value.
     * The acquisition of the resource is interruptible, as NIO resource acquisitions are often blocking
     * operations that should be interruptible.
     */
    def toManagedNio: ZManaged[R, E, A] = ZManaged.makeInterruptible(value)(_.close.ignore)

    /**
     * Provides bracketing for any effect producing a closeable NIO resource.
     * The acquisition of the resource is interruptible, as NIO resource acquisitions are often blocking
     * operations that should be interruptible.
     */
    def bracketNio[R1 <: R, E1 >: E, B](use: A => ZIO[R1, E1, B]): ZIO[R1, E1, B] =
      value.interruptible.bracket(_.close.ignore, use)

  }

  /**
   * Used with `ZStream#refineOrDie`, since `ZStream#refineToOrDie` does not exist yet.
   */
  val ioExceptionOnly: PartialFunction[Throwable, IOException] = {
    case io: IOException => io
  }

  implicit final class ManagedOps[-R, +E, +A](private val managed: ZManaged[R, E, A]) extends AnyVal {

    /**
     * Use this managed resource in an effect running in a forked fiber.
     * The resource will be released on the forked fiber after the effect exits,
     * whether it succeeds, fails or is interrupted.
     *
     * @param f The effect to run in a forked fiber. The resource is only valid within this effect.
     */
    def useForked[R2 <: R, E2 >: E, B](f: A => ZIO[R2, E2, B]): ZIO[R2, E, Fiber[E2, B]] =
      ReleaseMap.make.flatMap { releaseMap =>
        managed.zio.provideSome[R]((_, releaseMap)).flatMap { case (finalizer, a) => f(a).onExit(finalizer).fork }
      }
  }

}
