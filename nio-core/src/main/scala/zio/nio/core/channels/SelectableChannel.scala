package zio.nio.core.channels

import java.io.IOException
import java.net.{ SocketOption, ServerSocket => JServerSocket, Socket => JSocket }
import java.nio.channels.{
  SelectableChannel => JSelectableChannel,
  ServerSocketChannel => JServerSocketChannel,
  SocketChannel => JSocketChannel
}

import zio._
import zio.nio.core.channels.SelectionKey.Operation
import zio.nio.core.channels.spi.SelectorProvider
import zio.nio.core.SocketAddress

/**
 * A channel that can be in either blocking or non-blocking mode.
 */
trait ModalChannel extends Channel {

  protected val channel: JSelectableChannel

  final def provider: SelectorProvider = new SelectorProvider(channel.provider())

  final def isBlocking: UIO[Boolean] =
    IO.effectTotal(channel.isBlocking())

  final def blockingLock: UIO[AnyRef] =
    IO.effectTotal(channel.blockingLock())
}

/**
 * A channel that can be multiplexed via a [[zio.nio.core.channels.Selector]].
 */
trait SelectableChannel extends ModalChannel with WithEnv.NonBlocking {

  final def validOps: Set[Operation] =
    Operation.fromInt(channel.validOps())

  final def isRegistered: UIO[Boolean] =
    IO.effectTotal(channel.isRegistered())

  final def keyFor(sel: Selector): UIO[Option[SelectionKey]] =
    IO.effectTotal(Option(channel.keyFor(sel.selector)).map(new SelectionKey(_)))

  final def register(sel: Selector, ops: Set[Operation], att: Option[AnyRef]): IO[IOException, SelectionKey] =
    IO.effect(new SelectionKey(channel.register(sel.selector, Operation.toInt(ops), att.orNull)))
      .refineToOrDie[IOException]

  final def register(sel: Selector, ops: Set[Operation]): IO[IOException, SelectionKey] =
    IO.effect(new SelectionKey(channel.register(sel.selector, Operation.toInt(ops))))
      .refineToOrDie[IOException]

  final def register(sel: Selector, op: Operation, att: Option[AnyRef]): IO[IOException, SelectionKey] =
    IO.effect(new SelectionKey(channel.register(sel.selector, op.intVal, att.orNull)))
      .refineToOrDie[IOException]

  final def register(sel: Selector, op: Operation): IO[IOException, SelectionKey] =
    IO.effect(new SelectionKey(channel.register(sel.selector, op.intVal)))
      .refineToOrDie[IOException]

}

sealed abstract class SocketChannel[R](override protected[channels] val channel: JSocketChannel)
    extends ModalChannel
    with GatheringByteChannel[R]
    with ScatteringByteChannel[R] {

  def bind(local: SocketAddress): IO[IOException, Unit] =
    IO.effect(channel.bind(local.jSocketAddress)).refineToOrDie[IOException].unit

  def setOption[T](name: SocketOption[T], value: T): IO[IOException, Unit] =
    IO.effect(channel.setOption(name, value)).refineToOrDie[IOException].unit

  def shutdownInput: IO[IOException, Unit] =
    IO.effect(channel.shutdownInput()).refineToOrDie[IOException].unit

  def shutdownOutput: IO[IOException, Unit] =
    IO.effect(channel.shutdownOutput()).refineToOrDie[IOException].unit

  def socket: UIO[JSocket] =
    IO.effectTotal(channel.socket())

  def isConnected: UIO[Boolean] =
    IO.effectTotal(channel.isConnected)

  def remoteAddress: IO[IOException, Option[SocketAddress]] =
    IO.effect(Option(channel.getRemoteAddress()).map(SocketAddress.fromJava))
      .refineToOrDie[IOException]

  def localAddress: IO[IOException, Option[SocketAddress]] =
    IO.effect(Option(channel.getLocalAddress()).map(SocketAddress.fromJava))
      .refineToOrDie[IOException]
}

object SocketChannel {

  final class Blocking private[SocketChannel] (c: JSocketChannel)
      extends SocketChannel[blocking.Blocking](c)
      with GatheringByteChannel.Blocking
      with ScatteringByteChannel.Blocking {

    def nonBlockingMode: IO[IOException, NonBlocking] =
      IO.effect(c.configureBlocking(false))
        .refineToOrDie[IOException]
        .as(new NonBlocking(c))

    def connect(remote: SocketAddress): ZIO[blocking.Blocking, IOException, Unit] =
      IO.effect(channel.connect(remote.jSocketAddress)).refineToOrDie[IOException].unit

  }

  object Blocking {

    def fromJava(javaSocketChannel: JSocketChannel): Blocking =
      new Blocking(javaSocketChannel)

    def open: IO[IOException, Blocking] =
      IO.effect(fromJava(JSocketChannel.open())).refineToOrDie[IOException]

    def open(remote: SocketAddress): ZIO[blocking.Blocking, IOException, Blocking] =
      blocking
        .effectBlockingInterrupt(fromJava(JSocketChannel.open(remote.jSocketAddress)))
        .refineToOrDie[IOException]

  }

  final class NonBlocking private[SocketChannel] (c: JSocketChannel)
      extends SocketChannel[Any](c)
      with SelectableChannel {

    def blockingMode: IO[IOException, Blocking] =
      IO.effect(c.configureBlocking(true))
        .refineToOrDie[IOException]
        .as(new Blocking(c))

    def connect(remote: SocketAddress): IO[IOException, Boolean] =
      IO.effect(channel.connect(remote.jSocketAddress)).refineToOrDie[IOException]

    def isConnectionPending: UIO[Boolean] =
      IO.effectTotal(channel.isConnectionPending)

    def finishConnect: IO[IOException, Boolean] =
      IO.effect(channel.finishConnect()).refineToOrDie[IOException]

  }

  object NonBlocking {

    def fromJava(javaSocketChannel: JSocketChannel): NonBlocking =
      new NonBlocking(javaSocketChannel)

    def open: IO[IOException, NonBlocking] =
      IO.effect {
        val javaChannel = JSocketChannel.open()
        javaChannel.configureBlocking(false)
        fromJava(javaChannel)
      }.refineToOrDie[IOException]
  }
}

sealed abstract class ServerSocketChannel[R](override protected val channel: JServerSocketChannel)
    extends ModalChannel {

  /**
   * Binds this channel's socket to a local address and configures the socket to listen for connections.
   *
   * @param local The local socket address to bind to.
   * @param backlog The maximum number of pending connections, defaults to 0
   */
  def bindTo(local: SocketAddress, backlog: Int = 0): IO[IOException, Unit] = bind(Some(local), backlog)

  /**
   * Binds this channel's socket to an automatically assigned local address.
   * The `localAddress` method can be used to find out the exact socket address that was bound to.
   *
   * @param backlog The maximum number of pending connections, defaults to 0
   */
  def bindAuto(backlog: Int = 0): IO[IOException, Unit] = bind(None, backlog)

  /**
   * Binds this channel's socket to a local address and configures the socket to listen for connections.
   * The `localAddress` method can be used to find out the exact socket address that was bound to.
   *
   * @param local The local socket address to bind to. If not specified, binds to an automatically assigned address.
   * @param backlog The maximum number of pending connections, defaults to 0
   */
  def bind(local: Option[SocketAddress], backlog: Int = 0): IO[IOException, Unit] =
    IO.effect(channel.bind(local.map(_.jSocketAddress).orNull, backlog)).refineToOrDie[IOException].unit

  def setOption[T](name: SocketOption[T], value: T): IO[IOException, Unit] =
    IO.effect(channel.setOption(name, value)).refineToOrDie[IOException].unit

  def socket: UIO[JServerSocket] =
    IO.effectTotal(channel.socket())

  /**
   * Returns the socket address that this channel's socket is bound to.
   * Returns `None` if this socket is not yet bound to an address.
   */
  def localAddress: IO[IOException, Option[SocketAddress]] =
    IO.effect(Option(channel.getLocalAddress()).map(new SocketAddress(_))).refineToOrDie[IOException]

}

object ServerSocketChannel {

  final class Blocking private[ServerSocketChannel] (c: JServerSocketChannel)
      extends ServerSocketChannel[blocking.Blocking](c)
      with WithEnv.Blocking {

    def nonBlockingMode: IO[IOException, NonBlocking] =
      IO.effect(c.configureBlocking(false))
        .refineToOrDie[IOException]
        .as(new NonBlocking(c))

    /**
     * Accepts a connection made to this server socket.
     * Blocks until a connection is made.
     */
    def accept: ZIO[blocking.Blocking, IOException, SocketChannel.Blocking] =
      isBlocking.filterOrDie(identity)(new IllegalStateException("Blocking socket in non-blocking mode")) *>
        // the best way to cancel a blocking channel I/O operation is to close the channel
        blocking
          .effectBlockingCancelable(SocketChannel.Blocking.fromJava(c.accept()))(close.ignore)
          .refineToOrDie[IOException]

    /**
     * Accepts a connection which is used to create a forked effect value.
     * The effect value returned by `use` is run on a forked fiber.
     * The accept socket will be automatically closed once the fiber completes,
     * whether successful, failed or interrupted.
     */
    def acceptAndFork[R, A](
      use: SocketChannel.Blocking => ZIO[blocking.Blocking with R, IOException, A]
    ): ZIO[blocking.Blocking with R, IOException, Fiber[IOException, A]] =
      accept.toManagedNio.useForked(use)

  }

  object Blocking {

    def fromJava(javaChannel: JServerSocketChannel): Blocking =
      new Blocking(javaChannel)

    def open: IO[IOException, Blocking] =
      IO.effect(fromJava(JServerSocketChannel.open())).refineToOrDie[IOException]

  }

  final class NonBlocking private[ServerSocketChannel] (c: JServerSocketChannel)
      extends ServerSocketChannel[Any](c)
      with SelectableChannel {

    def blockingMode: IO[IOException, Blocking] =
      IO.effect(c.configureBlocking(true))
        .refineToOrDie[IOException]
        .as(new Blocking(c))

    private def assertNonBlocking =
      isBlocking.filterOrDie(!_)(new IllegalStateException("Non-blocking socket in blocking mode")).unit

    def accept: IO[IOException, Option[SocketChannel.Blocking]] =
      assertNonBlocking *>
        IO.effect(Option(c.accept()).map(SocketChannel.Blocking.fromJava)).refineToOrDie[IOException]

    def acceptNonBlocking: IO[IOException, Option[SocketChannel.NonBlocking]] =
      assertNonBlocking *>
        IO.effect {
          Option(c.accept()).map { javaChannel =>
            javaChannel.configureBlocking(false)
            SocketChannel.NonBlocking.fromJava(javaChannel)
          }
        }.refineToOrDie[IOException]

    def acceptBracket[R, A](f: SocketChannel.NonBlocking => ZIO[R, IOException, A]): ZIO[R, IOException, Option[A]] =
      acceptNonBlocking.some.flatMap(f.andThen(_.asSomeError)).optional

    def acceptBracket_[R](f: SocketChannel.NonBlocking => ZIO[R, IOException, Unit]): ZIO[R, IOException, Unit] =
      acceptNonBlocking.some.flatMap(f.andThen(_.asSomeError)).optional.someOrElse(())

    def acceptManaged: Managed[IOException, Option[SocketChannel.NonBlocking]] =
      Managed.make(acceptNonBlocking.some)(_.close.ignore).optional

  }

  object NonBlocking {

    def fromJava(javaChannel: JServerSocketChannel): NonBlocking = new NonBlocking(javaChannel)

    def open: IO[IOException, NonBlocking] =
      IO.effect {
        val javaChannel = JServerSocketChannel.open()
        javaChannel.configureBlocking(false)
        fromJava(javaChannel)
      }.refineToOrDie[IOException]

  }

}
