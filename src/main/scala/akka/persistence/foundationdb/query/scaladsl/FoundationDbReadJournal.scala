package akka.persistence.foundationdb.query.scaladsl

import java.util.concurrent.CompletableFuture

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.io.Udp.SO.Broadcast
import akka.persistence.PersistentRepr
import akka.persistence.foundationdb._
import akka.persistence.foundationdb.journal.{Key, KeySerializer, TagKey}
import akka.persistence.foundationdb.layers.{AssembledPayload, ChunkedValueAssembler}
import akka.persistence.foundationdb.query.VersionstampOffset
import akka.persistence.foundationdb.serialization.FdbSerializer
import akka.persistence.foundationdb.session.FoundationDbSession
import akka.persistence.foundationdb.util.{RangeRead, TupleOps}
import akka.persistence.foundationdb.util.TupleOps._
import akka.persistence.query.scaladsl._
import akka.persistence.query.{EventEnvelope, NoOffset, Offset, Sequence}
import akka.serialization.SerializationExtension
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.{Done, NotUsed}
import com.apple.foundationdb.{Database, FDBException, KeySelector, KeyValue, ReadTransaction, StreamingMode, Transaction, TransactionContext}
import com.apple.foundationdb.async.AsyncUtil
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.Tuple
import com.typesafe.config.Config

import scala.async.Async.{async, await}
import scala.compat.java8.FutureConverters._
import scala.concurrent.{Future, Promise}
import scala.collection.JavaConverters._

class FoundationDbReadJournal(system: ActorSystem, cfg: Config)
  extends ReadJournal
    with CurrentPersistenceIdsQuery
    with PersistenceIdsQuery
    with EventsByPersistenceIdQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByTagQuery
    with CurrentEventsByTagQuery {

  private[akka] val serialization = SerializationExtension(system)

  private[akka] implicit val ec = system.dispatcher

  private implicit val mat = ActorMaterializer()(system)

  private[akka] val config = new FoundationDbPluginConfig(system, cfg)

  import config._

  private[akka] val log: LoggingAdapter = Logging(system, getClass)

  private[akka] val fdbSerializer = new FdbSerializer(serialization)

  private[akka] val session = new FoundationDbSession(
    system,
    config.sessionProvider,
    log,
    config
  )

  private[akka] val directoriesFuture: Future[Directories] = session.resolveDirectories()

  private[akka] val keySerializerFuture: Future[KeySerializer] = directoriesFuture.map(d => new KeySerializer(d))

  def watch(key: Key)(implicit tx: Transaction): Future[Done] = {
    tx.watch(key.value.toArray).toScala.map(_ => Done)
  }

  val chunkAssembler = ChunkedValueAssembler()


  override def persistenceIds(): Source[String, NotUsed] = ???

  override def currentPersistenceIds(): Source[String, NotUsed] = {
//    val futureSource = directoriesFuture.map { directories =>
//      val range = directories.maxSeqNr.range()
//      RangeRead.longRunningRangeSource(range, None).map { kv =>
//        val persistentId = Tuple.fromBytes(kv.getKey).getString(1)
//        persistentId
//      }
//    }
//    Source.fromFutureSource(futureSource).mapMaterializedValue(_ => NotUsed)
    ???
  }

  override def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    require(fromSequenceNr <= toSequenceNr, "fromSequenceNr must be less or equal to toSequenceNr")

    val f = async {

      val keySerializer = await(keySerializerFuture)

     @volatile var currentSeqNo: Long = fromSequenceNr

      val toSeqNrValidated = if (toSequenceNr == Long.MaxValue) toSequenceNr else toSequenceNr + 1

      val (queue, newTagsAvailable) = Source.queue[Done](1, OverflowStrategy.dropNew).preMaterialize()

      queue.offer(Done)

      newTagsAvailable
        .mapAsync(1) { _ =>
          getTransaction().map { case fdbTr @ FdbTransaction(tr, _) =>
            watch(keySerializer.maxSequenceNr(persistenceId))(tr).foreach(e => queue.offer(e))
            fdbTr
          }
        }
        .flatMapConcat { case FdbTransaction(tr, trDone) =>
          implicit val transaction = tr
          val begin = keySerializer.message(persistenceId, currentSeqNo).value.toArray
          val end = keySerializer.message(persistenceId, toSeqNrValidated).value.toArray

          val source = RangeRead.rangeSource(
            begin = KeySelector.firstGreaterOrEqual(begin),
            end = KeySelector.firstGreaterOrEqual(end),
            mode = StreamingMode.WANT_ALL
          )
            .via(eventsByPersistenceIdFlow)
            .map { eventEnv =>
              currentSeqNo = eventEnv.offset.asInstanceOf[Sequence].value
              eventEnv
            }
            .watchTermination() { case (currentMat, streamResult) =>
              streamResult.onComplete { done =>
                trDone.trySuccess(Done)
              }
              currentMat
            }

          source
            .recoverWithRetries(-1, {
              case ex: FDBException if ex.getCode == 1007 =>
                queue.offer(Done) //we reached 5 sec timeout, restarting
                Source.empty
            })
        }

    }

    Source.fromFutureSource(f).mapMaterializedValue(_ => NotUsed)
  }

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    val f = async {
      implicit val db = await(session.underlying())
      val keySerializer = await(keySerializerFuture)
      val toSeqNrValidated = if (toSequenceNr == Long.MaxValue) toSequenceNr else toSequenceNr + 1
      val begin = keySerializer.message(persistenceId, fromSequenceNr).value.toArray
      val end = keySerializer.message(persistenceId, toSeqNrValidated).value.toArray
      RangeRead
        .longRunningRangeSource(begin, end, None)
        .via(eventsByPersistenceIdFlow)
    }
    Source.fromFutureSource(f).mapMaterializedValue(_ => NotUsed)
  }

  val eventsByPersistenceIdFlow: Flow[KeyValue, EventEnvelope, NotUsed] = {
    Flow[KeyValue]
      .via(chunkAssembler)
      .map { case AssembledPayload(_, value) =>
        val persistentRepr = fdbSerializer.bytes2PersistentRepr(value.toArray)
        EventEnvelope(
          Offset.sequence(persistentRepr.sequenceNr),
          persistentRepr.persistenceId,
          persistentRepr.sequenceNr,
          persistentRepr.payload
        )
      }
  }

  case class FdbTransaction(tx: Transaction, dbCompleteion: Promise[Done])

  def getTransaction(): Future[FdbTransaction] = {
    val p = Promise[Transaction]()
    val done = Promise[Done]()

    session.underlying().foreach { db =>
      db.runAsync { tx =>
        p.success(tx)
        done.future.toJava.toCompletableFuture
      }
    }
    p.future.map(tx => FdbTransaction(tx, done))
  }

  //todo test that it works with splitting events
  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {

    val f = async {
      val directories = await(directoriesFuture)

      val tagsDir = directories.tags

      val keySerializer = await(keySerializerFuture)

      @volatile var currentOffset: Offset = offset

      val (queue, newTagsAvailable) = Source.queue[Done](1, OverflowStrategy.dropNew).preMaterialize()

      queue.offer(Done)

      newTagsAvailable
        .mapAsync(1) { _ =>
          getTransaction().map { case fdbTr @ FdbTransaction(tr, _) =>
            watch(keySerializer.tagWatch(tag))(tr).foreach(e => queue.offer(e))
            fdbTr
          }
        }
        .flatMapConcat { case FdbTransaction(tr, trDone) =>
          implicit val transaction = tr
          val (begin, end) = currentOffset match {
            case versionstamp: VersionstampOffset =>
              val begin = keySerializer.tag(tag, versionstamp.value).value.toArray
              val end = tagsDir.range(Tuple.from(tag)).end
              begin -> end

            case NoOffset =>
              val begin = tagsDir.range(Tuple.from(tag)).begin
              val end = tagsDir.range(Tuple.from(tag)).end

              begin -> end

            case _ =>
              throw new IllegalArgumentException("FoundationDb does not support " + offset.getClass.getSimpleName + " offsets")
          }
          val source = RangeRead.rangeSource(
              begin = KeySelector.firstGreaterOrEqual(begin),
              end = KeySelector.firstGreaterOrEqual(end),
              mode = StreamingMode.WANT_ALL
            )
            .via(eventsByTagFlow)
            .map { eventEnv =>
              currentOffset = eventEnv.offset
              eventEnv
            }
            .watchTermination() { case (currentMat, streamResult) =>
              streamResult.onComplete { done =>
                trDone.trySuccess(Done)
              }
              currentMat
            }

          source
          .recoverWithRetries(-1, {
            case ex: FDBException if ex.getCode == 1007 =>
              queue.offer(Done) //we reached 5 sec timeout, restarting
              Source.empty
          })
        }

    }

    Source.fromFutureSource(f).mapMaterializedValue(_ => NotUsed)

  }



  private def getPersistentRepr(persistenceId: String, sequenceNr: Long): Future[Option[PersistentRepr]] = async {
    val keySerializer = await(keySerializerFuture)
    val key = keySerializer.message(persistenceId, sequenceNr)
    val range = key.subspace.range(key.tuple)
      await{
        session.readAsync { implicit tr =>
          RangeRead
            .rangeSource(
              range = range,
              limit = ReadTransaction.ROW_LIMIT_UNLIMITED,
              reverse = false,
              mode = StreamingMode.WANT_ALL
            )
            .via(chunkAssembler)
            .map(payload => fdbSerializer.bytes2PersistentRepr(payload.value.toArray))
            .runWith(Sink.headOption)
        }
      }
    }

  private val eventsByTagFlow: Flow[KeyValue, EventEnvelope, NotUsed] = {
    Flow[KeyValue]
      .via(chunkAssembler)
      .mapAsync(100) { case AssembledPayload(key, value) =>
        async {
          val keySerializer = await(keySerializerFuture)
          val tagKey = keySerializer.tag(key)
          fdbSerializer.bytes2TagType(value.toArray) match {
            case tag @ CompactTag(persistenceId, sequenceNr) =>
              await(getPersistentRepr(persistenceId, sequenceNr).map(_.map { persistentRepr =>
                EventEnvelope(
                  VersionstampOffset(tagKey.versionstamp),
                  persistentRepr.persistenceId,
                  persistentRepr.sequenceNr,
                  persistentRepr.payload
                )
              }.orElse { // no persistentRepr for a given persistenceId -> seqNr, looks like the event was deleted, let's remove it from the tag too
                session.runAsync { tr =>
                  tr.clear(tagKey.value.toArray)
                  Future.successful(Done)
                }
                None
              }))

            case RichTag(payload) =>
              val persistentRepr = fdbSerializer.bytes2PersistentRepr(payload)
              val envelope = EventEnvelope(
                VersionstampOffset(tagKey.versionstamp),
                persistentRepr.persistenceId,
                persistentRepr.sequenceNr,
                persistentRepr.payload
              )
              Some(envelope)
          }
        }
      }
      .mapConcat(_.toList)
  }


  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    val f = async {
      implicit val db = await(session.underlying())
      val keySerializer = await(keySerializerFuture)
      val tagsDir = await(directoriesFuture).tags
      val (begin, end) = offset match {
        case versionstamp: VersionstampOffset =>
          val begin = keySerializer.tag(tag, versionstamp.value).value.toArray
          val end = tagsDir.range(Tuple.from(tag)).end
          begin -> end

        case NoOffset =>
          val begin = tagsDir.range(Tuple.from(tag)).begin
          val end = tagsDir.range(Tuple.from(tag)).end
          begin -> end

        case _ =>
          throw new IllegalArgumentException("FoundationDb does not support " + offset.getClass.getSimpleName + " offsets")
      }

      //since we don't use any watches here, we can just create a new long running source and read it all.
      RangeRead
        .longRunningRangeSource(begin, end, None)
        .via(eventsByTagFlow)
    }
    Source.fromFutureSource(f).mapMaterializedValue(_ => NotUsed)
  }
}

object FoundationDbReadJournal {
  final val Identifier = "foundationdb-query-journal"
}
