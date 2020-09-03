package zio.nio.core.channels

import java.io.IOException
import java.net.{ ProtocolFamily, SocketOption, DatagramSocket => JDatagramSocket, SocketAddress => JSocketAddress }
import java.nio.channels.{ DatagramChannel => JDatagramChannel }

import zio.nio.core.channels.BlockingByteChannel.Streamable
import zio.{ IO, UIO, ZIO, blocking }
import zio.nio.core.{ ByteBuffer, SocketAddress }

/**
 * A channel for datagram-oriented sockets.
 *
 * A newly created datagram channel is open but not connected. The `send` and `receive` methods can be used without
 * needing to connect the channel. The channel must be connected to use `read` and `write` methods, and any methods
 * based on them such as `stream` and `sink`.
 */
sealed abstract class DatagramChannel[R] private[channels] (override protected[channels] val channel: JDatagramChannel)
    extends ModalChannel
    with ByteChannel[R]
    with WithEnv[R] {

  /**
   * Binds this channel's underlying socket to the given local address. Passing `None` binds to an
   * automatically assigned local address.
   *
   * @param local the local address
   * @return the datagram channel bound to the local address
   */
  def bind(local: Option[SocketAddress]): IO[IOException, Unit] = {
    val addr: JSocketAddress = local.map(_.jSocketAddress).orNull
    IO.effect(channel.bind(addr)).refineToOrDie[IOException].unit
  }

  /**
   * Connects this channel's underlying socket to the given remote address.
   *
   * @param remote the remote address
   */
  def connect(remote: SocketAddress): IO[IOException, Unit] =
    IO.effect(channel.connect(remote.jSocketAddress)).unit.refineToOrDie[IOException]

  /**
   * Disconnects this channel's underlying socket.
   */
  def disconnect: IO[IOException, Unit] =
    IO.effect(channel.disconnect()).unit.refineToOrDie[IOException]

  /**
   * Tells whether this channel's underlying socket is both open and connected.
   *
   * @return `true` when the socket is both open and connected, otherwise `false`
   */
  def isConnected: UIO[Boolean] =
    UIO.effectTotal(channel.isConnected())

  /**
   * Optionally returns the socket address that this channel's underlying socket is bound to.
   *
   * @return the local address if the socket is bound, otherwise `None`
   */
  def localAddress: IO[IOException, Option[SocketAddress]] =
    IO.effect(channel.getLocalAddress()).refineToOrDie[IOException].map(a => Option(a).map(new SocketAddress(_)))

  /**
   * Optionally returns the remote socket address that this channel's underlying socket is connected to.
   *
   * @return the remote address if the socket is connected, otherwise `None`
   */
  def remoteAddress: IO[IOException, Option[SocketAddress]] =
    IO.effect(channel.getRemoteAddress()).refineToOrDie[IOException].map(a => Option(a).map(new SocketAddress(_)))

  /**
   * Sets the value of the given socket option.
   *
   * @param name the socket option to be set
   * @param value the value to be set
   */
  def setOption[T](name: SocketOption[T], value: T): IO[IOException, Unit] =
    IO.effect(channel.setOption(name, value)).refineToOrDie[IOException].unit

  /**
   * Returns a reference to this channel's underlying datagram socket.
   *
   * @return the underlying datagram socket
   */
  def socket: UIO[JDatagramSocket] =
    IO.effectTotal(channel.socket())

}

object DatagramChannel {

  trait BlockingSendReceive[R] {
    this: WithEnv[R] =>

    protected[channels] val channel: JDatagramChannel

    /**
     * Receives a datagram via this channel into the given [[zio.nio.core.ByteBuffer]].
     * Blocks until a datagram is available.
     * Unlike `read*` methods, this can be used without first connecting this channel.
     * If there are fewer bytes remaining in the buffer than are required to hold the datagram then the remainder
     * of the datagram is silently discarded.
     *
     * @param dst the destination buffer
     * @return the socket address of the datagram's source.
     */
    def receive(dst: ByteBuffer): ZIO[R, IOException, SocketAddress] =
      withEnv {
        IO.effect(SocketAddress.fromJava(channel.receive(dst.byteBuffer))).refineToOrDie[IOException]
      }

    /**
     * Sends a datagram via this channel.
     * Blocks until the entire buffer is sent.
     * Unlike `write*` methods, this can be used without first connecting this channel.
     */
    def send(src: ByteBuffer, target: SocketAddress): ZIO[R, IOException, Unit] =
      withEnv {
        IO.effect(channel.send(src.byteBuffer, target.jSocketAddress)).refineToOrDie[IOException].unit
      }

  }

  final class Blocking private[DatagramChannel] (c: JDatagramChannel)
      extends DatagramChannel[blocking.Blocking](c)
      with BlockingByteChannel
      with BlockingSendReceive[blocking.Blocking] {

    self =>

    def nonBlockingMode: IO[IOException, NonBlocking] =
      IO.effect(c.configureBlocking(false))
        .refineToOrDie[IOException]
        .as(new NonBlocking(c))

    def datagramBlocking[R, E, A](
      f: Streamable with BlockingSendReceive[Any] => ZIO[R, E, A]
    ): ZIO[R with zio.blocking.Blocking, E, A] = {
      val channel = new ByteChannel[Any]
        with GatheringByteChannel.SinkWrite[Any]
        with ScatteringByteChannel.StreamRead[Any]
        with BlockingSendReceive[Any]
        with WithEnv.NonBlocking {
        override protected[channels] val channel = self.channel
      }
      WithEnv.withBlocking(channel)(f(channel))
    }
  }

  object Blocking {

    def fromJava(javaDatagramChannel: JDatagramChannel): Blocking =
      new Blocking(javaDatagramChannel)

    /**
     * Opens a new datagram channel.
     *
     * @return a new datagram channel
     */
    def open: IO[IOException, Blocking] =
      IO.effect(fromJava(JDatagramChannel.open())).refineToOrDie[IOException]

    /**
     * Opens a new datagram channel using a specified protocol family.
     */
    def open(family: ProtocolFamily): IO[IOException, Blocking] =
      IO.effect(fromJava(JDatagramChannel.open(family))).refineToOrDie[IOException]

  }

  final class NonBlocking private[DatagramChannel] (c: JDatagramChannel)
      extends DatagramChannel[Any](c)
      with SelectableChannel {

    def blockingMode: IO[IOException, Blocking] =
      IO.effect(c.configureBlocking(true))
        .refineToOrDie[IOException]
        .as(new Blocking(c))

    /**
     * Receives a datagram via this channel into the given [[zio.nio.core.ByteBuffer]].
     * If a datagram is not immediately available, `None` is returned and no data is copied to the buffer.
     * Unlike `read*` methods, this can be used without first connecting this channel.
     * If there are fewer bytes remaining in the buffer than are required to hold the datagram then the remainder
     * of the datagram is silently discarded.
     *
     * @param dst the destination buffer
     * @return the socket address of the datagram's source.
     */
    def receive(dst: ByteBuffer): IO[IOException, Option[SocketAddress]] =
      IO.effect(Option(c.receive(dst.byteBuffer)).map(SocketAddress.fromJava))
        .refineToOrDie[IOException]

    /**
     * Sends a datagram via this channel.
     * If the underlying output buffer did not have room for the datagram,
     * false is returned and the buffer is not modified.
     * Unlike `write*` methods, this can be used without first connecting this channel.
     */
    def send(src: ByteBuffer, target: SocketAddress): IO[IOException, Boolean] =
      IO.effect(c.send(src.byteBuffer, target.jSocketAddress) > 0)
        .refineToOrDie[IOException]

  }

  object NonBlocking {

    def fromJava(javaDatagramChannel: JDatagramChannel): NonBlocking =
      new NonBlocking(javaDatagramChannel)

    /**
     * Opens a new datagram channel.
     *
     * @return a new datagram channel
     */
    def open: IO[IOException, NonBlocking] =
      IO.effect {
        val javaChannel = JDatagramChannel.open()
        javaChannel.configureBlocking(false)
        fromJava(javaChannel)
      }.refineToOrDie[IOException]

    /**
     * Opens a new datagram channel using a specified protocol family.
     */
    def open(family: ProtocolFamily): IO[IOException, NonBlocking] =
      IO.effect {
        val javaChannel = JDatagramChannel.open(family)
        javaChannel.configureBlocking(false)
        fromJava(javaChannel)
      }.refineToOrDie[IOException]

  }

}
