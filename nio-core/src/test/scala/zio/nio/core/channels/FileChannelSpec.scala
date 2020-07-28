package zio.nio.core.channels

import java.io.EOFException
import java.nio.file.StandardOpenOption

import zio.{ Chunk, URIO, ZIO, blocking }
import zio.blocking.Blocking
import zio.nio.core.{ BaseSpec, Buffer }
import zio.nio.core.charset.Charset
import zio.nio.core.file.{ Files, Path }
import zio.stream.Stream
import zio.test._
import zio.test.Assertion._

import scala.io.Source

object FileChannelSpec extends BaseSpec {

  private val readFile = Path("nio-core/src/test/resources/async_file_read_test.txt")

  private val readFileContents = "Hello World"

  def loadViaSource(path: Path): URIO[Blocking, List[String]] =
    ZIO
      .effect(Source.fromFile(path.toFile))
      .bracket(s => ZIO.effectTotal(s.close()))(s => blocking.effectBlocking(s.getLines().toList))
      .orDie

  override def spec =
    suite("FileChannelSpec")(
      testM("asynchronous file buffer read") {
        for {
          channel <- AsynchronousFileChannel.open(readFile, StandardOpenOption.READ)
          buffer  <- Buffer.byte(16)
          _       <- channel.read(buffer, 0)
          _       <- buffer.flip
          array   <- buffer.array
          text     = array.takeWhile(_ != 10).map(_.toChar).mkString.trim
        } yield assert(text)(equalTo(readFileContents))
      },
      testM("asynchronous file chunk read") {
        for {
          channel <- AsynchronousFileChannel.open(readFile, StandardOpenOption.READ)
          bytes   <- channel.readChunk(500, 0L)
          chars   <- Charset.Standard.utf8.decodeChunk(bytes)
        } yield assert(chars.mkString)(equalTo(readFileContents))
      },
      testM("asynchronous file write") {
        val path     = Path("nio-core/src/test/resources/async_file_write_test.txt")
        val zChannel = AsynchronousFileChannel
          .open(
            path,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE
          )
        for {
          channel <- zChannel
          buffer  <- Buffer.byte(Chunk.fromArray("Hello World".getBytes))
          _       <- channel.write(buffer, 0)
          result  <- loadViaSource(path)
          _       <- Files.delete(path)
        } yield assert(result.size)(equalTo(1)) && assert(result.head)(equalTo(readFileContents))
      },
      testM("memory mapped buffer") {
        for {
          result <- FileChannel
                      .open(readFile, StandardOpenOption.READ)
                      .bracket(_.close.ignore) { channel =>
                        for {
                          buffer <- channel.map(FileChannel.MapMode.READ_ONLY, 0L, 6L)
                          bytes  <- buffer.getChunk()
                          chars  <- Charset.Standard.utf8.decodeChunk(bytes)
                        } yield assert(chars.mkString)(equalTo(readFileContents.take(6)))
                      }
        } yield result
      },
      testM("end of stream") {
        FileChannel
          .open(readFile, StandardOpenOption.READ)
          .bracketNio { channel =>
            for {
              size <- channel.size
              _    <- channel.readChunk(size.toInt)
              _    <- channel.readChunk(1)
            } yield ()
          }
          .flip
          .map(assert(_)(isSubtype[EOFException](anything)))
      },
      testM("stream reading") {
        FileChannel
          .open(readFile, StandardOpenOption.READ)
          .bracketNio {
            _.stream().transduce(Charset.Standard.utf8.newDecoder.transducer()).runCollect.map(_.mkString)
          }
          .map(assert(_)(equalTo(readFileContents)))
      },
      testM("sink writing") {
        val testData =
          """Yet such is oft the course of deeds that move the wheels of the world:
            | small hands do them because they must, while the eyes of the great are elsewhere.""".stripMargin
        val stream   = Stream.fromIterable(testData).transduce(Charset.Standard.utf8.newEncoder.transducer())
        val file     = Path("nio-core/src/test/resources/sink_write_test.txt")
        FileChannel
          .open(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
          .bracketNio(channel => stream.run(channel.sink()))
          .zipRight(loadViaSource(file).ensuring(Files.delete(file).orDie))
          .map(lines => assert(lines.mkString("\n"))(equalTo(testData)))
      }
    )
}
