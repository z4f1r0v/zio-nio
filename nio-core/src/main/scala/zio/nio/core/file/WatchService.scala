package zio.nio.core.file

import java.io.IOException
import java.nio.file.{
  WatchEvent,
  Path => JPath,
  Watchable => JWatchable,
  WatchKey => JWatchKey,
  WatchService => JWatchService
}
import java.util.concurrent.TimeUnit

import zio.{ IO, UIO, ZIO }
import zio.blocking
import zio.blocking.Blocking
import zio.duration.Duration
import zio.nio.core.IOCloseable
import zio.stream.{ ZSink, ZStream }

import scala.jdk.CollectionConverters._

trait Watchable {
  protected def javaWatchable: JWatchable

  final def register(watcher: WatchService, events: WatchEvent.Kind[_]*): IO[IOException, WatchKey] =
    IO.effect(new WatchKey(javaWatchable.register(watcher.javaWatchService, events: _*))).refineToOrDie[IOException]

  final def register(
    watcher: WatchService,
    events: Iterable[WatchEvent.Kind[_]],
    modifiers: WatchEvent.Modifier*
  ): IO[IOException, WatchKey] =
    IO.effect(new WatchKey(javaWatchable.register(watcher.javaWatchService, events.toArray, modifiers: _*)))
      .refineToOrDie[IOException]
}

object Watchable {

  def apply(jWatchable: JWatchable): Watchable =
    new Watchable {
      override protected val javaWatchable = jWatchable
    }
}

final class WatchKey private[file] (private val javaKey: JWatchKey) {

  def isValid: UIO[Boolean] = UIO.effectTotal(javaKey.isValid)

  def pollEvents: UIO[List[WatchEvent[_]]] = UIO.effectTotal(javaKey.pollEvents().asScala.toList)

  def pollEventsStream: ZStream[Any, Nothing, WatchEvent[_]] =
    ZStream.fromJavaIteratorTotal(javaKey.pollEvents().iterator())

  def reset: UIO[Boolean] = UIO.effectTotal(javaKey.reset())

  def cancel: UIO[Unit] = UIO.effectTotal(javaKey.cancel())

  def watchable: Watchable =
    javaKey.watchable() match {
      case javaPath: JPath => Path.fromJava(javaPath)
      case javaWatchable   => Watchable(javaWatchable)
    }

  /**
   * Convenience method to process the events pending on this `WatchKey` with automatic reset.
   *
   * Once all of the pending events have been processed by the provided sink, this `WatchKey` will be reset,
   * making it available to be enqueued in the `WatchService` again.
   */
  def process[R, E, A](sink: ZSink[R, E, WatchEvent[_], Any, A]): ZIO[R, E, A] =
    pollEventsStream.run(sink).ensuring(reset)
}

/**
 * A watch service that watches registered objects for changes and events.
 *
 * For example a file manager may use a watch service to monitor a directory for changes so that it can update its
 * display of the list of files when files are created or deleted.
 *
 * Note if any of the methods, or a stream returned by the `stream` method, is used after this `WatchService`
 * has been closed, the operation will die with a `ClosedWatchServiceException`.
 * It is recommended use to use the resource management provided by [[zio.nio.core.CloseableResourceOps]] to safely
 * acquire and release resources.
 */
final class WatchService private (private[file] val javaWatchService: JWatchService) extends IOCloseable[Any] {

  override def close: IO[IOException, Unit] = IO.effect(javaWatchService.close()).refineToOrDie[IOException]

  def poll: UIO[Option[WatchKey]] =
    IO.effectTotal(Option(javaWatchService.poll()).map(new WatchKey(_)))

  def poll(timeout: Duration): IO[InterruptedException, Option[WatchKey]] =
    IO.effect(Option(javaWatchService.poll(timeout.toNanos, TimeUnit.NANOSECONDS)).map(new WatchKey(_)))
      .refineToOrDie[InterruptedException]

  def take: ZIO[Blocking, InterruptedException, WatchKey] =
    blocking
      .effectBlockingInterrupt(new WatchKey(javaWatchService.take()))
      .refineToOrDie[InterruptedException]

  /**
   * A stream of signalled objects which have pending events.
   *
   * Note the `WatchKey` objects returned by this stream must be reset before they will be
   * queued again with any additional events.
   */
  def stream: ZStream[Blocking, InterruptedException, WatchKey] = ZStream.repeatEffect(take)

}

object WatchService {
  def forDefaultFileSystem: ZIO[Blocking, IOException, WatchService] = FileSystem.default.newWatchService

  def fromJava(javaWatchService: JWatchService): WatchService = new WatchService(javaWatchService)
}
