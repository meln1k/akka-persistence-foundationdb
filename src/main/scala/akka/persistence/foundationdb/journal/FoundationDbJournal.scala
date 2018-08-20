package akka.persistence.foundationdb.journal

import java.nio.{ByteBuffer, ByteOrder}

import akka.Done
import akka.persistence.foundationdb.util.RangeRead
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.persistence.journal.{AsyncWriteJournal, Tagged}
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.ByteString
import com.apple.foundationdb.{MutationType, Transaction}
import com.apple.foundationdb.tuple.Versionstamp
import com.typesafe.config.Config

import scala.collection.immutable
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.compat.java8.FutureConverters._
import scala.util.control.NonFatal
import akka.event.{Logging, LoggingAdapter}
import akka.persistence.foundationdb.{CompactTag, Directories, TagType}
import akka.persistence.foundationdb.serialization.{FdbSerializer, SerializedMessage}
import akka.persistence.foundationdb.session.FoundationDbSession

import scala.async.Async._
import scala.concurrent.duration._
import scala.concurrent.blocking
import akka.persistence.foundationdb.layers._

class FoundationDbJournal(cfg: Config) extends AsyncWriteJournal {

  private[akka] val config = new FoundationDbJournalConfig(context.system, cfg)

  private[akka] val serialization = SerializationExtension(context.system)
  private[akka] val log: LoggingAdapter = Logging(context.system, getClass)

  import config._

  val fdbSerializer = new FdbSerializer(serialization)

  implicit val dispatcher =
    context.system.dispatchers.lookup("foundationdb-plugin-default-dispatcher")

  implicit val mat = ActorMaterializer(ActorMaterializerSettings(context.system))

  val session = new FoundationDbSession(
    context.system,
    config.sessionProvider,
    log,
    config
  )

  //super bad, but only once //TODO fixit
  val directories: Directories = blocking {
    Await.result(session.resolveDirectories(), 10.seconds)
  }

  val keySerializer = new KeySerializer(directories)

  private val chunkedByteStringReader = ChunkedValueAssembler()

//  def insertCompactTag(tag: String, persistenceId: String, sequenceNr: Long, messageNr: Int)(implicit tr: Transaction): Unit = {
//    tr.mutate(
//      MutationType.SET_VERSIONSTAMPED_VALUE,
//      directories.tagWatches.pack(Tuple.from(tag)),
//      ByteString.newBuilder
//        .putBytes(Versionstamp.incomplete(messageNr).getBytes)
//        .result()
//        .toArray
//    )
//    tr.mutate(
//      MutationType.SET_VERSIONSTAMPED_KEY,
//      directories.tags.packWithVersionstamp(Tuple.from(tag, Versionstamp.incomplete(messageNr))),
//      persistentReprId2Tuple(persistenceId, sequenceNr).pack()
//    )
//  }

  def insertTag(tag: String, tagType: TagType, messageNr: Int)(implicit tr: Transaction): Unit = {
    tr.mutate(
      MutationType.SET_VERSIONSTAMPED_VALUE,
      keySerializer.tagWatch(tag).bytes,
      ByteString.newBuilder
        .putBytes(Versionstamp.incomplete(messageNr).getBytes) //versionstamp
        .putInt(0)(ByteOrder.LITTLE_ENDIAN) //position where it's located
        .result()
        .toArray
    )
    BlobLayer.writeChunkedWithVersionstamp(
      keySerializer.tag(tag, Versionstamp.incomplete(messageNr)),
      fdbSerializer.tagType2bytes(tagType)
    )
  }

//  def insertRichTag(tr: Transaction, tag: String, persistentRepr: PersistentRepr, messageNr: Int): Unit = {
//    val shardId = TagWatchShards.get(tag).map(s => persistentRepr.persistenceId.hashCode % s).getOrElse(0)
//    tr.mutate(
//      MutationType.SET_VERSIONSTAMPED_VALUE,
//      tagWatchKey(directories.tagWatches, tag, shardId),
//      ByteString.newBuilder
//        .putBytes(Versionstamp.incomplete(messageNr).getBytes)
//        .result()
//        .toArray
//    )
//    tr.mutate(
//      MutationType.SET_VERSIONSTAMPED_KEY,
//      directories.tags.packWithVersionstamp(Tuple.from(tag, Versionstamp.incomplete(messageNr), EVENT_TAG_RICH: java.lang.Long)),
//      Tuple.from(fdbSerializer.persistentRepr2Bytes(persistentRepr)).pack()
//    )

//    ???
//  }

  def insertCompactMessage(persistentRepr: PersistentRepr)(implicit tx: Transaction): Unit = {
    val serializedMessage =
      fdbSerializer.serializePersistentRepr(persistentRepr)
    writeSerializedMessage(serializedMessage)
  }

  def insertRichMessage(tr: Transaction, persistentRepr: PersistentRepr, messageNr: Int): Unit = {
//    val serializedMessage = fdbSerializer.serializePersistentRepr(persistentRepr, Rich(messageNr))
//    writeSerializedMessage(tr, serializedMessage)
    ???
  }

  // writing the serialized message using (directoryØ, persistenceId, seqNr, chunkNr) -> binary chunk
  def writeSerializedMessage(serializedMessage: SerializedMessage)(implicit tx: Transaction): Unit = {
    val key = keySerializer.message(serializedMessage.persistenceId, serializedMessage.sequenceNr)
    BlobLayer.writeChunked(key, serializedMessage.payload)
  }

  val storePersistentRepr: (Transaction, PersistentRepr, Set[String], Int) => Unit =
    tagStoringPolicy match {
//      case TagStoringPolicy.DefaultCompact(richTags) =>
//        (tr, persistentRepr, tags, messageNr) =>
//        val currentRichTags = richTags intersect tags
//        val noRichTags = currentRichTags.isEmpty
//        val currentCompactTags = if (noRichTags) {
//          tags
//        } else {
//          tags -- richTags
//        }
//        currentRichTags.foreach(richTag => insertRichTag(tr, richTag, persistentRepr, messageNr))
//        currentCompactTags.foreach(compactTag => insertCompactTag(tr, compactTag, persistentRepr.persistenceId, persistentRepr.sequenceNr, messageNr))
//        if (noRichTags) {
//          insertCompactMessage(tr, persistentRepr)
//        } else {
//          insertRichMessage(tr, persistentRepr, messageNr)
//        }

      case TagStoringPolicy.AlwaysCompact =>
        (tr, persistentRepr, tags, messageNr) =>
          tags.foreach { tag =>
            val tagType = CompactTag(persistentRepr.persistenceId, persistentRepr.sequenceNr)
            insertTag(tag, tagType, messageNr)(tr)
          }
          insertCompactMessage(persistentRepr)(tr)

//      case TagStoringPolicy.DefaultRich(compactTags) =>
//        (tr, persistentRepr, tags, messageNr) =>
//        val currentCompactTags = compactTags intersect tags
//        val noCompactTags = currentCompactTags.isEmpty
//        val currentRichTags = if (noCompactTags) {
//          tags
//        } else {
//          tags -- compactTags
//        }
//        currentCompactTags.foreach(compactTag => insertCompactTag(tr, compactTag, persistentRepr.persistenceId, persistentRepr.sequenceNr, messageNr))
//        currentRichTags.foreach(richTag => insertRichTag(tr, richTag, persistentRepr, messageNr))
//        insertRichMessage(tr, persistentRepr, messageNr)
//
//      case TagStoringPolicy.AlwaysRich =>
//        (tr, persistentRepr, tags, messageNr) =>
//        tags.foreach(tag => insertRichTag(tr, tag, persistentRepr, messageNr))
//        insertRichMessage(tr, persistentRepr, messageNr)
    }

  def storeMaxSequenceNr(persistentId: String, sequenceNr: Long)(implicit tr: Transaction) = {
    tr.mutate(
      MutationType.MAX,
      keySerializer.maxSequenceNr(persistentId).bytes,
      ByteString.newBuilder
        .putLong(sequenceNr)(ByteOrder.LITTLE_ENDIAN)
        .result()
        .toArray
    )

  }

  def isWriteSafe(persistentId: String, sequenceNr: Long)(implicit tx: Transaction): Future[Boolean] = async {
    val key = keySerializer.message(persistentId, sequenceNr)
    val firstChunk = key.subspace.pack(key.tuple.add(0))
    val message = await(tx.get(firstChunk).toScala)
    message match {
      case null => true
      case _    => false
    }
  }

  override def postStop(): Unit = {
    session.close()
  }

  private def asyncWriteSafe(payload: immutable.Seq[PersistentRepr])(implicit tr: Transaction): Future[Done] = {

    val futures = payload.zipWithIndex.map {
      case (persistentRepr, messageNr) =>
        val canCommit = isWriteSafe(persistentRepr.persistenceId, persistentRepr.sequenceNr)
        canCommit.map {
          case true =>
            val (prWithoutTags, tags) = persistentRepr.payload match {
              case Tagged(payload, tags) =>
                (persistentRepr.withPayload(payload), tags)
              case _ =>
                (persistentRepr, Set.empty[String])
            }

            storePersistentRepr(tr, prWithoutTags, tags, messageNr.toInt)
            storeMaxSequenceNr(persistentRepr.persistenceId, persistentRepr.sequenceNr)
          case false =>
            tr.cancel()
            throw new IllegalStateException(
              "Multiple writers with the same persistenceId were detected. " +
                s"Rejecting conflicting write for the message with persistenceId ${persistentRepr.persistenceId} " +
                s"and sequenceNr ${persistentRepr.sequenceNr} made by ${persistentRepr.writerUuid}")
        }
    }

    Future.sequence(futures).map(_ => Done)

  }

  private def asyncWriteFast(payload: immutable.Seq[PersistentRepr])(implicit tr: Transaction): Future[Done] = {
    payload.zipWithIndex.foreach {
      case (persistentRepr, messageNr) =>
        val (prWithoutTags, tags) = persistentRepr.payload match {
          case Tagged(payload, tags) =>
            (persistentRepr.withPayload(payload), tags)
          case _ =>
            (persistentRepr, Set.empty[String])
        }
        storePersistentRepr(tr, prWithoutTags, tags, messageNr)
        storeMaxSequenceNr(persistentRepr.persistenceId, persistentRepr.sequenceNr)
    }
    Future.successful(Done)
  }

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {

    val futures = messages.map {
      case AtomicWrite(payload) =>
        session
          .runAsync { implicit tr =>
            if (config.checkJournalCorruption) {
              asyncWriteSafe(payload)
            } else {
              asyncWriteFast(payload)
            }
          }
          .map(_ => Success(()))
          .recover {
            case NonFatal(ex) =>
              Failure(ex)
          }
    }

    Future.sequence(futures)

  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    tagStoringPolicy match {
      case TagStoringPolicy.AlwaysCompact =>
        val normalizedSeqNo = toSequenceNr match {
          case Long.MaxValue => toSequenceNr
          case _             => toSequenceNr + 1
        }
        session.runAsync { tr =>
          val from = keySerializer.message(persistenceId, 0L).bytes
          val to = keySerializer.message(persistenceId, normalizedSeqNo).bytes
          tr.clear(from, to)
          Future.successful(())
        }

      case _ =>
        throw new UnsupportedOperationException("can only delete from AlwaysCompact tags")
    }
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit): Future[Unit] = {
    val from = keySerializer.message(persistenceId, fromSequenceNr).bytes
    val normalizedToSeqNo = toSequenceNr match {
      case Long.MaxValue => toSequenceNr
      case _             => toSequenceNr + 1
    }
    val to = keySerializer.message(persistenceId, normalizedToSeqNo).bytes

    //TODO: correctly handle values which can't fit into Int
    val limit = max match {
      case Long.MaxValue                => None
      case long if long >= Int.MaxValue => Some(Int.MaxValue)
      case int                          => Some(int.toInt)
    }

    session.underlying().flatMap { implicit tx =>
      RangeRead
        .longRunningRangeSource(from, to, limit)
        .via(chunkedByteStringReader)
        .map(bs => fdbSerializer.bytes2PersistentRepr(bs.value.toArray))
        .map(recoveryCallback)
        .runWith(Sink.ignore)
        .map(_ => ())
    }

  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    session
      .runAsync { tr =>
        val key = keySerializer.maxSequenceNr(persistenceId).bytes
        tr.get(key).toScala
      }
      .map {
        case null =>
          0L
        case bytes: Array[Byte] =>
          readLittleEndianLong(bytes)
      }
  }

  private def readLittleEndianLong(bytes: Array[Byte]): Long = {
    ByteBuffer
      .wrap(bytes)
      .order(ByteOrder.LITTLE_ENDIAN)
      .getLong
  }
}
