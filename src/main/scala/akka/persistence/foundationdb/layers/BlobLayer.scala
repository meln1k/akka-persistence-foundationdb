package akka.persistence.foundationdb.layers

import akka.NotUsed
import akka.persistence.foundationdb.journal.{VersionstampedKey, Key}
import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString
import com.apple.foundationdb.{KeyValue, MutationType, Transaction}
import com.apple.foundationdb.tuple.Tuple

object BlobLayer {

  val chunkSize = 10240

  def writeChunked(key: Key, payload: Array[Byte])(implicit tx: Transaction): Unit = {
    val tuple = key.tuple
    payload.grouped(chunkSize).zipWithIndex.foreach {
      case (chunk, index) =>
        tx.set(key.subspace.pack(tuple.add(index)), chunk)
    }
  }
  def writeChunkedWithVersionstamp(key: Key with VersionstampedKey, payload: Array[Byte])(
      implicit tx: Transaction): Unit = {
    val tuple = key.tuple
    payload.grouped(chunkSize).zipWithIndex.foreach {
      case (chunk, index) =>
        tx.mutate(
          MutationType.SET_VERSIONSTAMPED_KEY,
          key.subspace.packWithVersionstamp(tuple.add(index)),
          chunk
        )
    }
  }
}

case class AssembledPayload(key: ByteString, value: ByteString)

class ChunkedValueAssembler(maxSingleElementSize: Int) extends GraphStage[FlowShape[KeyValue, AssembledPayload]] {
  val in = Inlet[KeyValue]("ChunkedByteStringReader.in")
  val out = Outlet[AssembledPayload]("ChunkedByteStringReader.out")

  override def shape: FlowShape[KeyValue, AssembledPayload] =
    FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private var buffer = ByteString.empty
      private var currentKey = ByteString.empty
      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            val key = elem.getKey
            val tuple = Tuple.fromBytes(key)
            val chunkNr = tuple.getLong(tuple.size() - 1) // last element of the tuple
            if (chunkNr == 0 && buffer.nonEmpty) { // new message started
              push(out, AssembledPayload(currentKey, buffer))
              buffer = ByteString.fromArrayUnsafe(elem.getValue)
              currentKey = ByteString.fromArrayUnsafe(tuple.popBack().pack())
            } else {
              buffer ++= ByteString.fromArrayUnsafe(elem.getValue)
              if (currentKey.isEmpty) {
                currentKey = ByteString.fromArrayUnsafe(tuple.popBack().pack())
              }
              if (buffer.size > maxSingleElementSize) {
                failStage(new IllegalStateException("Buffer overflow"))
              } else {
                pull(in)
              }
            }
          }

          override def onUpstreamFinish(): Unit = {
            if (buffer.nonEmpty) {
              emit(out, AssembledPayload(currentKey, buffer))
            }
            completeStage()
          }
        }
      )

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }
}

object ChunkedValueAssembler {
  // 10 megabytes is the current FDB transaction limit
  final val FDB_MAX_TRANSACTION_SIZE = 10 * 1024 * 1024
  def apply(maxBufferSize: Int = FDB_MAX_TRANSACTION_SIZE): Flow[KeyValue, AssembledPayload, NotUsed] = {
    Flow.fromGraph(new ChunkedValueAssembler(maxBufferSize))
  }
}
