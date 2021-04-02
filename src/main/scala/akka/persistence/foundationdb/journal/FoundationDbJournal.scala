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
import akka.persistence.foundationdb.{CompactTag, Directories, RichTag, TagType}
import akka.persistence.foundationdb.serialization.{FdbSerializer, SerializedMessage}
import akka.persistence.foundationdb.session.FoundationDbSession

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

  def insertMessage(persistentRepr: PersistentRepr)(implicit tx: Transaction): Unit = {
    val serializedMessage = fdbSerializer.serializePersistentRepr(persistentRepr)
    writeSerializedMessage(serializedMessage)
  }

  // writing the serialized message using (directoryØ, persistenceId, seqNr, chunkNr) -> binary chunk
  def writeSerializedMessage(serializedMessage: SerializedMessage)(implicit tx: Transaction): Unit = {
    val key = keySerializer.message(serializedMessage.persistenceId, serializedMessage.sequenceNr)
    BlobLayer.writeChunked(key, serializedMessage.payload)
  }

  def storePersistentRepr(persistentRepr: PersistentRepr, tags: Set[String], messageNr: Int)(
      implicit tr: Transaction): Unit =
    tagStoringPolicy match {
      case TagStoringPolicy.DefaultCompact(richTags) =>
        val currentRichTags = richTags intersect tags
        val noRichTags = currentRichTags.isEmpty
        val currentCompactTags = if (noRichTags) {
          tags
        } else {
          tags -- richTags
        }
        currentRichTags.foreach { richTag =>
          val tagType = RichTag(fdbSerializer.persistentRepr2Bytes(persistentRepr))
          insertTag(richTag, tagType, messageNr)
        }
        currentCompactTags.foreach { compactTag =>
          val tagType = CompactTag(persistentRepr.persistenceId, persistentRepr.sequenceNr)
          insertTag(compactTag, tagType, messageNr)
        }
        insertMessage(persistentRepr)

      case TagStoringPolicy.AlwaysCompact =>
        tags.foreach { tag =>
          val tagType = CompactTag(persistentRepr.persistenceId, persistentRepr.sequenceNr)
          insertTag(tag, tagType, messageNr)
        }
        insertMessage(persistentRepr)

      case TagStoringPolicy.DefaultRich(compactTags) =>
        val currentCompactTags = compactTags intersect tags
        val noCompactTags = currentCompactTags.isEmpty
        val currentRichTags = if (noCompactTags) {
          tags
        } else {
          tags -- compactTags
        }
        currentCompactTags.foreach { compactTag =>
          val tagType = CompactTag(persistentRepr.persistenceId, persistentRepr.sequenceNr)
          insertTag(compactTag, tagType, messageNr)
        }
        currentRichTags.foreach { richTag =>
          val tagType = RichTag(fdbSerializer.persistentRepr2Bytes(persistentRepr))
          insertTag(richTag, tagType, messageNr)
        }
        insertMessage(persistentRepr)

      case TagStoringPolicy.AlwaysRich =>
        tags.foreach { tag =>
          val tagType = RichTag(fdbSerializer.persistentRepr2Bytes(persistentRepr))
          insertTag(tag, tagType, messageNr)
        }
        insertMessage(persistentRepr)
    }

  def storeMaxSequenceNr(persistentId: String, sequenceNr: Long)(implicit tr: Transaction): Unit = {
    tr.mutate(
      MutationType.MAX,
      keySerializer.maxSequenceNr(persistentId).bytes,
      ByteString.newBuilder
        .putLong(sequenceNr)(ByteOrder.LITTLE_ENDIAN)
        .result()
        .toArray
    )

  }

  def isWriteSafe(persistentId: String, sequenceNr: Long)(implicit tx: Transaction): Future[Boolean] = {
    val key = keySerializer.message(persistentId, sequenceNr)
    val firstChunk = key.subspace.pack(key.tuple.add(0))
    for {
      message <- tx.get(firstChunk).toScala
      result = message match {
        case null => true
        case _    => false
      }
    } yield result
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

            storePersistentRepr(prWithoutTags, tags, messageNr.toInt)
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
        storePersistentRepr(prWithoutTags, tags, messageNr)
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
