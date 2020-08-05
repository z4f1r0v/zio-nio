package zio.nio.core
package channels

import zio.{ IO, ZIO, blocking }

private[channels] trait WithEnv[R] {

  /**
   * Performs an effect using some environment `R`.
   * There are two implementations:
   *  - `WithEnv.NonBlocking` returns the effect as-is with an environment of `Any`
   *  - `WithEnv.Blocking` returns the effect using ZIO's blocking thread pool from environment `zio.blocking.Blocking`.
   *    Such blocking effects can be interrupted.
   */
  protected def withEnv[E, A](effect: IO[E, A]): ZIO[R, E, A]

}

private[channels] object WithEnv {

  trait Blocking extends WithEnv[blocking.Blocking] {

    this: IOCloseable[blocking.Blocking] =>

    /**
     * Performs a blocking NIO channel operation on the blocking thread pool and respecting interruption.
     * If the fiber is interrupted, the blocking operation will be cancelled by closing the channel.
     * Interrupting (in the Java sense) the blocked thread would also cancel the operation,
     * but this results in the channel being closed anyway, and is more expensive for ZIO to arrange.
     */
    override protected def withEnv[E, A](effect: IO[E, A]): ZIO[blocking.Blocking, E, A] =
      blocking.blocking(effect).fork.flatMap(_.join).onInterrupt(close.ignore)
  }

  trait NonBlocking extends WithEnv[Any] {
    override protected def withEnv[E, A](effect: IO[E, A]): ZIO[Any, E, A] = effect
  }

}
