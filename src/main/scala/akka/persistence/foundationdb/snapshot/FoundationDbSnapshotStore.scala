package akka.persistence.foundationdb.snapshot

import akka.event.{Logging, LoggingAdapter}
import akka.persistence.foundationdb.journal.{Key, KeySerializer}
import akka.persistence.foundationdb.{Directories, FoundationDbPluginConfig}
import akka.persistence.foundationdb.layers.{BlobLayer, ChunkedValueAssembler}
import akka.persistence.foundationdb.serialization.FdbSerializer
import akka.persistence.foundationdb.session.FoundationDbSession
import akka.persistence.foundationdb.util.RangeRead
import akka.persistence.serialization.Snapshot
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import akka.persistence.snapshot.SnapshotStore
import akka.serialization.SerializationExtension
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.stream.scaladsl.Sink
import com.apple.foundationdb.{KeySelector, StreamingMode}
import com.apple.foundationdb.directory.DirectorySubspace
import com.apple.foundationdb.tuple.Tuple
import com.typesafe.config.Config

import scala.concurrent.{Await, Future, blocking}
import scala.concurrent.duration._

class FoundationDbSnapshotStore(cfg: Config) extends SnapshotStore {

  private[akka] val config = new FoundationDbPluginConfig(context.system, cfg)

  private[akka] val serialization = SerializationExtension(context.system)

//  import config._


  val fdbSerializer = new FdbSerializer(serialization)

  implicit val dispatcher = context.system.dispatchers.lookup("foundationdb-plugin-default-dispatcher")

  implicit val mat = ActorMaterializer(ActorMaterializerSettings(context.system))

  val session = new FoundationDbSession(
    context.system,
    config.sessionProvider,
    log,
    config
  )

  //super bad, but it will happen only once //TODO fixit
  val directories: Directories = blocking {
    Await.result(session.resolveDirectories(), 10.seconds)
  }

  val keySerializer = new KeySerializer(directories)


  private val chunkedByteStringReader = ChunkedValueAssembler()

  def serializeSnapshot(snapshot: Snapshot): Array[Byte] = serialization.findSerializerFor(snapshot).toBinary(snapshot)

  def deserializeSnapshot(bytes: Array[Byte]): Snapshot = serialization.deserialize(bytes, classOf[Snapshot]).get

  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    session.readAsync { implicit tx =>


      val from = keySerializer.snapshot(persistenceId, criteria.minSequenceNr, criteria.minTimestamp).value.toArray

      val to = keySerializer.snapshot(persistenceId, criteria.maxSequenceNr, criteria.maxTimestamp+1).value.toArray

      RangeRead.rangeSource(
        begin = KeySelector.firstGreaterOrEqual(from),
        end = KeySelector.firstGreaterOrEqual(to),
        limit = 1,
        reverse = true,
        mode = StreamingMode.EXACT
      )
        .via(chunkedByteStringReader)
        .map { assembled =>
          val tuple = Tuple.fromBytes(assembled.key.toArray)
          val seqNr = tuple.getLong(2)
          val timestamp = tuple.getLong(3)
          SelectedSnapshot(
            SnapshotMetadata(persistenceId, seqNr, timestamp),
            deserializeSnapshot(assembled.value.toArray).data
          )
        }
        .runWith(Sink.headOption)
    }
  }

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    val bytes = serializeSnapshot(Snapshot(snapshot))
    session.runAsync { implicit tx =>
      BlobLayer.writeChunked(
        keySerializer.snapshot(metadata.persistenceId, metadata.sequenceNr, metadata.timestamp),
        bytes
      )
      Future.successful(())
    }
  }

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    val key = keySerializer.snapshot(metadata.persistenceId, metadata.sequenceNr: java.lang.Long, metadata.timestamp: java.lang.Long)
    session.runAsync { implicit tx =>
      tx.clear(key.subspace.range(key.tuple))
      Future.successful(())
    }
  }

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    val from = keySerializer.snapshot(persistenceId, criteria.minSequenceNr, criteria.minTimestamp).value.toArray
    val to = keySerializer.snapshot(persistenceId, criteria.maxSequenceNr, criteria.maxTimestamp+1).value.toArray
    session.runAsync { implicit tx =>
      tx.clear(from, to)
      Future.successful(())
    }
  }
}
