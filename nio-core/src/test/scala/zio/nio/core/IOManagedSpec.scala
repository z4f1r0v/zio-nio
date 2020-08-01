package zio.nio.core

import java.io.IOException

import zio.{ IO, Ref, ZIO }
import zio.duration._
import zio.test.{ suite, testM, _ }
import zio.test.Assertion._

object IOManagedSpec extends BaseSpec {

  final private class TestResource(ref: Ref[Boolean]) extends IOCloseable[Any] {

    override def close: IO[IOException, Unit] = ref.set(true)

  }

  private val fakeResource = new IOCloseable[Any] {

    override def close: ZIO[Any, IOException, Unit] = ZIO.dieMessage("should not be called")

  }

  override def spec =
    suite("IOManagedSpec")(
      testM("ZManaged value calls close") {
        for {
          ref    <- Ref.make(false)
          _      <- IO.succeed(new TestResource(ref)).toManagedNio.use(_ => IO.unit)
          result <- ref.get
        } yield assert(result)(isTrue)
      },
      testM("bracket calls close") {
        for {
          ref    <- Ref.make(false)
          _      <- IO.succeed(new TestResource(ref)).bracketNio(_ => IO.unit)
          result <- ref.get
        } yield assert(result)(isTrue)
      },
      testM("ZManaged acquisition is interruptible") {
        ZIO
          .sleep(2.minutes)
          .as(fakeResource)
          .toManagedNio
          .useNow
          .fork
          .flatMap(_.interrupt)
          .map(exit => assert(exit)(isInterrupted))
      },
      testM("Bracket acquisition is interruptible") {
        ZIO
          .sleep(2.minutes)
          .as(fakeResource)
          .bracketNio(_ => IO.dieMessage("Should not happen"))
          .fork
          .flatMap(_.interrupt)
          .map(exit => assert(exit)(isInterrupted))
      }
    )

}
