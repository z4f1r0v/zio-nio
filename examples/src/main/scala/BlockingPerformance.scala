package zio
package nio
package examples

import java.io.IOException
import java.nio.file.StandardOpenOption

import core.file.{ Files, Path }
import zio.blocking.Blocking
import zio.nio.core.channels.FileChannel
import stream.Stream
import zio.clock.Clock
import zio.console.Console
import zio.random.Random

/**
 * Demonstrates how to minimise the overhead of using blocking NIO calls in ZIO effect values.
 *
 * To correctly encapsulate a blocking NIO call in a ZIO effect value we need two things:
 *  - switch the fiber to a thread from the blocking pool, to avoid consuming a thread from
 *   the fixed-size default pool
 *  - fork and join the effect to allow attaching interrupt handling
 *
 * When making blocking calls directly on a channel, these overhead costs must be paid for each call, as
 * in addition to switching the fiber back to the default pool after the effect completes.
 * In non-trivial applications, this can significantly harm performance.
 *
 * To minimise the blocking overhead, variety of methods are available to perform a sequence of blocking
 * NIO calls, paying the overhead cost only once for the entire sequence. Such a sequence can also be
 * recursive/looping, such as long running read/write loops. The methods are:
 *  - `blockingWrites` - available on all writable channels
 *  - `blockingReads` - available on all readable channels
 *  - `blocking` - available on channels that can read and write, supporting both reads and writes in one effect value
 *  - `fileBlocking` - available on `FileChannel`, as per `blocking` but with the file-specific methods available
 *
 * This program writes 1 million individual bytes using two styles:
 *  - The first calls write directly on the `FileChannel`, which results in the blocking overhead costs described above
 *   being payed 1 million times
 *  - The second uses `fileBlocking`, which results in the blocking overhead costs being payed once
 *
 * This test is deliberately extreme by writing single bytes. Larger buffer sizes tend to reduce the overhead
 * of style 1 simply because there are fewer writes. However, there are situation where the buffer size can't
 * be changed, or doing so imposes other costs.
 */
object BlockingPerformance extends App {

  private val oneMeg = 1000000L

  def performanceTest(file: Path): ZIO[Blocking with Random with Clock with Console, IOException, Unit] = {
    val data = Stream.repeat(42.toByte).take(oneMeg)
    FileChannel
      .open(file, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
      .bracketNio { fileChan =>
        for {
          _                   <- console.putStr("Running without `fileBlocking`... ")
          result1             <- data.run(fileChan.sink()).timed
          _                   <- console.putStrLn("done")
          _                   <- console.putStr("Running with `fileBlocking`... ")
          result2             <- fileChan.fileBlocking(chan => data.run(chan.sink())).timed
          _                   <- console.putStrLn("done")
          (duration1, length1) = result1
          (duration2, length2) = result2
          _                   <-
            console.putStrLn(f"Without using `fileBlocking`: $length1%,d bytes written in ${duration1.toMillis}%,7dms")
          _                   <-
            console.putStrLn(f"Using `fileBlocking`        : $length2%,d bytes written in ${duration2.toMillis}%,7dms")
        } yield ()
      }
      .zipRight(Files.delete(file))
  }

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    args.headOption
      .map(Path(_))
      .map(performanceTest)
      .getOrElse(console.putStrLn("Specify a file to write random data to"))
      .exitCode

}
