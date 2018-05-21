package akka.persistence.foundationdb.journal

import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.CompletableFuture

import akka.persistence.foundationdb.util.RangeRead
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.persistence.journal.{AsyncWriteJournal, Tagged}
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.ByteString
import com.apple.foundationdb.{MutationType, Transaction}
import com.apple.foundationdb.async.AsyncUtil
import com.apple.foundationdb.tuple.{Tuple, Versionstamp}
import com.typesafe.config.Config
import akka.persistence.foundationdb.util.TupleOps._

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.compat.java8.FutureConverters._
import scala.util.control.NonFatal
import TagStoringPolicy._

class FoundationDbJournal(cfg: Config) extends AsyncWriteJournal {


  val config = new FoundationDbJournalConfig(cfg)

  import config._


  val serialization = SerializationExtension(context.system)

  import context.dispatcher

  implicit val mat = ActorMaterializer(ActorMaterializerSettings(context.system))

  def persistentRepr2Bytes(p: PersistentRepr): Array[Byte] = serialization.serialize(p).get
  def bytes2PersistentRepr(bytes: Array[Byte]): PersistentRepr = serialization.deserialize(bytes, classOf[PersistentRepr]).get

  def insertCompactTag(tr: Transaction, tag: String, persistenceId: String, sequenceNr: Long, messageNr: Int): Unit = {
    tr.mutate(
      MutationType.SET_VERSIONSTAMPED_KEY,
      tagsDir.packWithVersionstamp(Tuple.from(tag, Versionstamp.incomplete(messageNr), EVENT_TAG_COMPACT: java.lang.Long)),
      persistentReprId2Tuple(persistenceId, sequenceNr).pack()
    )
  }

  def insertRichTag(tr: Transaction, tag: String, persistentRepr: PersistentRepr, messageNr: Int): Unit = {
    tr.mutate(
      MutationType.SET_VERSIONSTAMPED_KEY,
      tagsDir.packWithVersionstamp(Tuple.from(tag, Versionstamp.incomplete(messageNr), EVENT_TAG_RICH: java.lang.Long)),
      Tuple.from(persistentRepr2Bytes(persistentRepr)).pack()
    )
  }

  def insertCompactEventLog(tr: Transaction, persistentRepr: PersistentRepr): Unit = {
    tr.set(
      logsDir.pack(Tuple.from(persistentRepr.persistenceId, persistentRepr.sequenceNr: java.lang.Long, EVENT_TAG_COMPACT: java.lang.Long)),
       persistentRepr2Bytes(persistentRepr)
    )
  }

  def insertRichEventLog(tr: Transaction, persistentRepr: PersistentRepr, messageNr: Int): Unit = {
    tr.mutate(
      MutationType.SET_VERSIONSTAMPED_VALUE,
      logsDir.pack(Tuple.from(persistentRepr.persistenceId, persistentRepr.sequenceNr: java.lang.Long, EVENT_TAG_RICH: java.lang.Long)),
      {
        ByteString.newBuilder
          .putBytes(Versionstamp.incomplete(messageNr).getBytes)
          .putBytes(persistentRepr2Bytes(persistentRepr))
          .result()
          .toArray
      }
    )
  }

  val storePersistentRepr: (Transaction, PersistentRepr, Set[String], Int) => Unit =
    tagStoringPolicy match {
      case TagStoringPolicy.DefaultCompact(richTags) =>
        (tr, persistentRepr, tags, messageNr) =>
        val currentRichTags = richTags intersect tags
        val noRichTags = currentRichTags.isEmpty
        val currentCompactTags = if (noRichTags) {
          tags
        } else {
          tags -- richTags
        }
        currentRichTags.foreach(richTag => insertRichTag(tr, richTag, persistentRepr, messageNr))
        currentCompactTags.foreach(compactTag => insertCompactTag(tr, compactTag, persistentRepr.persistenceId, persistentRepr.sequenceNr, messageNr))
        if (noRichTags) {
          insertCompactEventLog(tr, persistentRepr)
        } else {
          insertRichEventLog(tr, persistentRepr, messageNr)
        }

      case TagStoringPolicy.AlwaysCompact =>
        (tr, persistentRepr, tags, messageNr) =>
        tags.foreach(tag => insertCompactTag(tr, tag, persistentRepr.persistenceId, persistentRepr.sequenceNr, messageNr))
        insertCompactEventLog(tr, persistentRepr)

      case TagStoringPolicy.DefaultRich(compactTags) =>
        (tr, persistentRepr, tags, messageNr) =>
        val currentCompactTags = compactTags intersect tags
        val noCompactTags = currentCompactTags.isEmpty
        val currentRichTags = if (noCompactTags) {
          tags
        } else {
          tags -- compactTags
        }
        currentCompactTags.foreach(compactTag => insertCompactTag(tr, compactTag, persistentRepr.persistenceId, persistentRepr.sequenceNr, messageNr))
        currentRichTags.foreach(richTag => insertRichTag(tr, richTag, persistentRepr, messageNr))
        insertRichEventLog(tr, persistentRepr, messageNr)

      case TagStoringPolicy.AlwaysRich =>
        (tr, persistentRepr, tags, messageNr) =>
        tags.foreach(tag => insertRichTag(tr, tag, persistentRepr, messageNr))
        insertRichEventLog(tr, persistentRepr, messageNr)
    }


  def storeMaxSequenceNr(tr: Transaction, persistentId: String, sequenceNr: Long) = {
    tr.mutate(
      MutationType.BYTE_MAX,
      seqNoDir.pack(Tuple.from(persistentId)),
      ByteString.newBuilder.putLong(sequenceNr)(ByteOrder.LITTLE_ENDIAN).result().toArray
    )

  }

  override def postStop(): Unit = {
    db.close()
  }

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {

      val futures = messages.map { case AtomicWrite(payload) =>
        db.runAsync { tr =>
          payload.zipWithIndex.foreach { case (persistentRepr, messageNr) =>
            val (prWithoutTags, tags) = persistentRepr.payload match {
              case Tagged(payload, tags) =>
                (persistentRepr.withPayload(payload), tags)
              case _ =>
                (persistentRepr, Set.empty[String])
            }
            storePersistentRepr(tr, prWithoutTags, tags, messageNr)
            storeMaxSequenceNr(tr, persistentRepr.persistenceId, persistentRepr.sequenceNr)
          }
          AsyncUtil.DONE
        }
          .toScala
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
          case _ => toSequenceNr + 1
        }
        db.runAsync { tr =>
          val from = logsDir.pack(persistentReprId2Tuple(persistenceId, 0L))
          val to   = logsDir.pack(persistentReprId2Tuple(persistenceId, normalizedSeqNo))
          tr.clear(from, to)
          CompletableFuture.completedFuture(())
        }.toScala

      case _ =>
        throw new UnsupportedOperationException("can only delete from AlwaysCompact tags")
    }
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(recoveryCallback: PersistentRepr => Unit): Future[Unit] = {
    val from = logsDir.pack(persistentReprId2Tuple(persistenceId, fromSequenceNr))
    val to = logsDir.pack(persistentReprId2Tuple(persistenceId, toSequenceNr + 1)) //range reads exclude the last key

    //TODO: correctly handle values which can't fit into Int
    val limit = max match {
      case Long.MaxValue => None
      case long if long >= Int.MaxValue => Some(Int.MaxValue)
      case int => Some(int.toInt)
    }

    RangeRead
      .longRunningRangeSource(from, to, limit)
      .map { kv =>
        val key = Tuple.fromBytes(kv.getKey)
        key.getLong(3) match {
          case EVENT_TAG_COMPACT =>
            bytes2PersistentRepr(kv.getValue)
          case EVENT_TAG_RICH =>
            bytes2PersistentRepr(kv.getValue.drop(Versionstamp.LENGTH))
        }
      }
      .map(recoveryCallback)
      .runWith(Sink.ignore)
      .map(_ => ())

  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    db.runAsync {tr =>
        val key = seqNoDir.pack(Tuple.from(persistenceId))
        tr.get(key)
      }
      .toScala
      .map {
        case null =>
          0L
        case bytes: Array[Byte] =>
          ByteBuffer
            .wrap(bytes)
            .order(ByteOrder.LITTLE_ENDIAN)
            .getLong
      }
  }
}
