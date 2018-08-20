package akka.persistence.foundationdb.serialization

/*
 * Copyright (C) 2018 Nikita Melkozerov. <n.melkozerov at gmail dot com>
 */

import akka.Done
import akka.actor.ActorSystem
import akka.persistence.foundationdb.layers.{AssembledPayload, ChunkedValueAssembler}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.ByteString
import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.tuple.Tuple
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext.Implicits.global

class ChunkedValueAssemblerSpec
    extends TestKit(ActorSystem("ChunkedByteStringReaderSpec"))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  implicit val mat = ActorMaterializer()

  "ChunkedByteStringReaderSpec" must {
    "assemble a single message" in {

      val chunks = (0 to 10).map { n =>
        Tuple.from("message", n: java.lang.Integer).pack() -> Array(n.toByte)
      }

      val keyValues = chunks.map { case (k, v) => new KeyValue(k, v) }

      val result = Source(keyValues)
        .via(ChunkedValueAssembler())
        .runWith(Sink.head)
        .futureValue
      result shouldEqual AssembledPayload(
        ByteString.fromArray(Tuple.from("message").pack()),
        chunks.map { case (_, v) => ByteString.fromArray(v) }.reduceLeft(_ ++ _)
      )
    }

    "assemble multiple messages" in {

      val chunkGroups = (1 to 10).map { groupNr =>
        (0 to 100).map { chunkNr =>
          Tuple
            .from(s"group-$groupNr", chunkNr: java.lang.Integer)
            .pack() -> Array(chunkNr.toByte)
        }
      }

      val keyValues = chunkGroups.flatMap { chunks =>
        chunks.map {
          case (k, v) =>
            new KeyValue(k, v)
        }
      }

      val result = Source(keyValues)
        .via(ChunkedValueAssembler())
        .runWith(Sink.seq)
        .futureValue
      result shouldEqual chunkGroups.zipWithIndex.map {
        case (group, id) =>
          AssembledPayload(
            ByteString.fromArray(Tuple.from(s"group-${id + 1}").pack()),
            ByteString.fromArray(group.flatMap(_._2).toArray)
          )
      }
    }

    "assemble a message from a single chunk" in {
      val keyValue =
        new KeyValue(Tuple.from("message", 0: java.lang.Integer).pack(), ByteString("payload").toArray)
      val result = Source(List(keyValue))
        .via(ChunkedValueAssembler())
        .runWith(Sink.seq)
        .futureValue
      result shouldEqual Seq(
        AssembledPayload(ByteString.fromArray(Tuple.from("message").pack()), ByteString("payload")))
    }

    "assemble multiple single chunk messages" in {

      val messages = (1 to 3).map(n => s"message$n" -> s"payload$n")
      val keyValues = messages.map {
        case (k, v) =>
          new KeyValue(Tuple.from(k, 0: java.lang.Integer).pack(), ByteString(v).toArray)
      }
      val assembledPayloads = messages.map {
        case (k, v) =>
          AssembledPayload(ByteString.fromArray(Tuple.from(k).pack()), ByteString(v)),
      }

      val result = Source(keyValues)
        .via(ChunkedValueAssembler())
        .runWith(Sink.seq)
        .futureValue
      result shouldEqual assembledPayloads
    }

    "drop the stream if an element is bigger than a limit" in {
      val chunks = (0 to 10).map { n =>
        Tuple.from("message", n: java.lang.Integer).pack() -> Array(n.toByte)
      }

      val keyValues = chunks.map { case (k, v) => new KeyValue(k, v) }

      val result = Source(keyValues)
        .via(ChunkedValueAssembler(9))
        .runWith(Sink.head)
        .recover {
          case _: IllegalStateException => Done
        }
        .futureValue

      result shouldEqual Done

    }
    "handle an empty source" in {
      val result = Source(List.empty[KeyValue])
        .via(ChunkedValueAssembler())
        .runWith(Sink.seq)
        .futureValue

      result shouldEqual Seq.empty

    }
  }

}
