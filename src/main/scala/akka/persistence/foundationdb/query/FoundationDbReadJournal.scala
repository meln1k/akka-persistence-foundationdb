package akka.persistence.foundationdb.query

import java.util.concurrent.CompletableFuture

import akka.{Done, NotUsed}
import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.persistence.PersistentRepr
import akka.persistence.foundationdb.journal.FoundationDbJournalConfig
import akka.persistence.foundationdb.util.RangeRead
import akka.persistence.query.{EventEnvelope, NoOffset, Offset}
import akka.persistence.query.scaladsl._
import akka.stream.scaladsl.Source
import com.apple.foundationdb.tuple.{Tuple, Versionstamp}
import com.typesafe.config.Config
import akka.persistence.foundationdb.journal.TagStoringPolicy._
import akka.persistence.foundationdb.util.KeySerializers.tagWatchKey
import akka.persistence.foundationdb.util.TupleOps._
import akka.serialization.SerializationExtension
import com.apple.foundationdb.TransactionContext
import com.apple.foundationdb.async.AsyncUtil

import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

class FoundationDbReadJournal(system: ActorSystem, cfg: Config)
  extends ReadJournal
    with PersistenceIdsQuery
    with CurrentPersistenceIdsQuery
    with EventsByPersistenceIdQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByTagQuery
    with CurrentEventsByTagQuery {

  val serialization = SerializationExtension(system)

  implicit val ec = system.dispatcher

  val config = new FoundationDbJournalConfig(cfg)

  import config._

  def watch(key: Array[Byte])(implicit tcx: TransactionContext): Future[Done] = {
    tcx.runAsync { tr =>
      CompletableFuture.completedFuture(tr.watch(key))
    }.toScala.map(_.toScala.map(_ => Done)).flatten
  }

  override def persistenceIds(): Source[String, NotUsed] = ???

  override def currentPersistenceIds(): Source[String, NotUsed] = {
    RangeRead.longRunningRangeSource(seqNoDir.)
  }

  override def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = ???

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = ???

  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    @volatile var currentOffset: Offset = offset
    val tagShards = TagWatchShards.getOrElse(tag, 1)
    val shardIds = Iterator.from(0).take(tagShards)
    Source.repeat(NotUsed)
      .mapAsync(1) { _ =>
        Future.firstCompletedOf(shardIds.map(id => watch(tagWatchKey(tagWatchDir, tag, id))))
      }
      .prepend(Source.single(Done)) //to start without any watch triggered
      .flatMapConcat(_ => currentEventsByTag(tag, currentOffset))
      .map { eventEnv =>
        currentOffset = eventEnv.offset
        eventEnv
      }
  }

  private def getPersistentRepr(tuple: Tuple): Future[Option[PersistentRepr]] = {
    val key = logsDir.pack(tuple)

    val tr: CompletableFuture[Array[Byte]] = db.readAsync { tr =>
      tr.get(key)
    }

    tr.toScala.map {
      case null => //deleted
        None
      case bytes =>
        Some(serialization.deserialize(bytes, classOf[PersistentRepr]).get)
    }
  }

  private def eventsSource(begin: Array[Byte], end: Array[Byte]): Source[EventEnvelope, NotUsed] = {
    RangeRead.longRunningRangeSource(begin, end, None).mapAsync(100) {kv =>
      val tuple = Tuple.fromBytes(kv.getKey)
      val versionstamp = tuple.getVersionstamp(2)
      tuple.getLong(3) match {
        case EVENT_TAG_COMPACT =>
          val valueTuple = Tuple.fromBytes(kv.getValue)
          val persistenceId = valueTuple.getString(0)
          val seqNr = valueTuple.getLong(1)
          getPersistentRepr(valueTuple)
            .map(_.map { persistentRepr =>
              EventEnvelope(
                VersionstampBasedOffset(versionstamp),
                persistenceId,
                seqNr,
                persistentRepr.payload
              )
            }.orElse { //lazily clear tags in case of broken key
              db.runAsync { tr =>
                tr.clear(kv.getKey)
                AsyncUtil.DONE
              }
              None
            })

        case EVENT_TAG_RICH =>
          throw new NotImplementedError("will be here sometime")
      }
    }
      .mapConcat(_.toList)
  }

  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    offset match {
      case stamp: VersionstampBasedOffset =>
        val begin = tagsDir.subspace(Tuple.from(Tuple.from(tag, stamp.value))).pack()
        val end = tagsDir.subspace(Tuple.from(tag)).range().end
        eventsSource(begin, end)

      case NoOffset  =>
        val begin = tagsDir.subspace(Tuple.from(tag)).range().begin
        val end = tagsDir.subspace(Tuple.from(tag)).range().end
        eventsSource(begin, end)

      case _ =>
        throw new IllegalArgumentException("LevelDB does not support " + offset.getClass.getSimpleName + " offsets")
    }

  }
}
