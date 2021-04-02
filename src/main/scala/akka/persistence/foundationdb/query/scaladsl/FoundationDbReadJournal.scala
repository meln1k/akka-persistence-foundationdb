package akka.persistence.foundationdb.query.scaladsl

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.persistence.PersistentRepr
import akka.persistence.foundationdb._
import akka.persistence.foundationdb.journal.{Key, KeySerializer}
import akka.persistence.foundationdb.layers.{AssembledPayload, ChunkedValueAssembler}
import akka.persistence.foundationdb.query.{FoundationDbReadJournalConfig, VersionstampOffset}
import akka.persistence.foundationdb.serialization.FdbSerializer
import akka.persistence.foundationdb.session.FoundationDbSession
import akka.persistence.foundationdb.util.RangeRead
import akka.persistence.query.scaladsl._
import akka.persistence.query.{EventEnvelope, NoOffset, Offset, Sequence}
import akka.serialization.SerializationExtension
import akka.stream._
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import com.apple.foundationdb.{FDBException, KeySelector, KeyValue, ReadTransaction, StreamingMode, Transaction}
import com.apple.foundationdb.tuple.{Tuple, Versionstamp}
import com.typesafe.config.Config

import scala.compat.java8.FutureConverters._
import scala.concurrent.{Future, Promise}

class FoundationDbReadJournal(system: ActorSystem, cfg: Config)
    extends ReadJournal
    with CurrentPersistenceIdsQuery
    with EventsByPersistenceIdQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByTagQuery
    with CurrentEventsByTagQuery {

  private val serialization = SerializationExtension(system)

  private implicit val ec = system.dispatcher

  private implicit val mat = ActorMaterializer()(system)

  private val writePluginId = cfg.getString("write-plugin")

  private val config = new FoundationDbReadJournalConfig(system, system.settings.config.getConfig(writePluginId))

  private val log: LoggingAdapter = Logging(system, getClass)

  private val fdbSerializer = new FdbSerializer(serialization)

  private val session = new FoundationDbSession(
    system,
    config.sessionProvider,
    log,
    config
  )

  private[akka] val directoriesFuture: Future[Directories] =
    session.resolveDirectories()

  private[akka] val keySerializerFuture: Future[KeySerializer] =
    directoriesFuture.map(d => new KeySerializer(d))

  private def watch(key: Key)(implicit tx: Transaction): Future[Done] = {
    tx.watch(key.bytes).toScala.map(_ => Done)
  }

  private val chunkAssembler = ChunkedValueAssembler()

  override def currentPersistenceIds(): Source[String, NotUsed] = {
    val f = for {
      directories <- directoriesFuture
      db <- session.underlying()
      range = directories.maxSeqNr.range()
    } yield
      RangeRead.longRunningRangeSource(range, None)(db, ec).map { kv =>
        val persistentId = Tuple.fromBytes(kv.getKey).getString(1)
        persistentId
      }

    Source.futureSource(f).mapMaterializedValue(_ => NotUsed)
  }

  override def eventsByPersistenceId(persistenceId: String,
                                     fromSequenceNr: Long,
                                     toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    require(fromSequenceNr <= toSequenceNr, "fromSequenceNr must be less or equal to toSequenceNr")

    @volatile var currentSeqNo: Long = fromSequenceNr

    val toSeqNrValidated =
      if (toSequenceNr == Long.MaxValue) toSequenceNr else toSequenceNr + 1

    val (queue, newTagsAvailable) =
      Source.queue[Done](1, OverflowStrategy.dropNew).preMaterialize()

    queue.offer(Done)

    val f: Future[Source[EventEnvelope, NotUsed]] = keySerializerFuture.map { keySerializer =>
      newTagsAvailable
        .mapAsync(1) { _ =>
          getTransaction().map {
            case fdbTr @ FdbTransaction(tr, _) =>
              watch(keySerializer.maxSequenceNr(persistenceId))(tr).foreach(e => queue.offer(e))
              fdbTr
          }
        }
        .flatMapConcat {
          case FdbTransaction(tr, trDone) =>
            implicit val transaction = tr
            val begin = if (currentSeqNo != fromSequenceNr) {
              keySerializer.message(persistenceId, currentSeqNo + 1).bytes
            } else {
              keySerializer.message(persistenceId, currentSeqNo).bytes
            }

            val end = keySerializer.message(persistenceId, toSeqNrValidated).bytes

            val source = RangeRead
              .rangeSource(
                begin = KeySelector.firstGreaterOrEqual(begin),
                end = KeySelector.firstGreaterOrEqual(end),
                mode = StreamingMode.WANT_ALL
              )
              .via(eventsByPersistenceIdFlow)
              .map { eventEnv =>
                currentSeqNo = eventEnv.offset.asInstanceOf[Sequence].value
                eventEnv
              }
              .watchTermination() {
                case (currentMat, streamResult) =>
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

    Source.futureSource(f).mapMaterializedValue(_ => NotUsed)
  }

  override def currentEventsByPersistenceId(persistenceId: String,
                                            fromSequenceNr: Long,
                                            toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    val toSeqNrValidated =
      if (toSequenceNr == Long.MaxValue) toSequenceNr else toSequenceNr + 1

    val f = for {
      db <- session.underlying()
      keySerializer <- keySerializerFuture
      begin = keySerializer.message(persistenceId, fromSequenceNr).bytes
      end = keySerializer.message(persistenceId, toSeqNrValidated).bytes
    } yield
      RangeRead
        .longRunningRangeSource(begin, end, None)(db, ec)
        .via(eventsByPersistenceIdFlow)
    Source.futureSource(f).mapMaterializedValue(_ => NotUsed)
  }

  private val eventsByPersistenceIdFlow: Flow[KeyValue, EventEnvelope, NotUsed] = {
    Flow[KeyValue]
      .via(chunkAssembler)
      .map {
        case AssembledPayload(_, value) =>
          val persistentRepr = fdbSerializer.bytes2PersistentRepr(value.toArray)
          EventEnvelope(
            Offset.sequence(persistentRepr.sequenceNr),
            persistentRepr.persistenceId,
            persistentRepr.sequenceNr,
            persistentRepr.payload
          )
      }
  }

  private case class FdbTransaction(tx: Transaction, dbCompleteion: Promise[Done])

  private def getTransaction(): Future[FdbTransaction] = {
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

    @volatile var currentOffset: Offset = offset

    val (queue, newTagsAvailable) =
      Source.queue[Done](1, OverflowStrategy.dropNew).preMaterialize()

    queue.offer(Done)

    val f = for {
      directories <- directoriesFuture
      tagsDir = directories.tags
      keySerializer <- keySerializerFuture
      result = newTagsAvailable
        .mapAsync(1) { _ =>
          getTransaction().map {
            case fdbTr @ FdbTransaction(tr, _) =>
              watch(keySerializer.tagWatch(tag))(tr).foreach(e => queue.offer(e))
              fdbTr
          }
        }
        .flatMapConcat {
          case FdbTransaction(tr, trDone) =>
            implicit val transaction = tr
            val (begin, end) = currentOffset match {
              case versionstamp: VersionstampOffset =>
                val begin = if (currentOffset != offset) {
                  keySerializer
                    .tag(
                      tag,
                      Versionstamp.complete(
                        versionstamp.value.getTransactionVersion,
                        versionstamp.value.getUserVersion + 1
                      ) // we need to skip the current event
                    )
                    .bytes
                } else {
                  keySerializer.tag(tag, versionstamp.value).bytes
                }

                val end = tagsDir.range(Tuple.from(tag)).end
                begin -> end

              case NoOffset =>
                val begin = tagsDir.range(Tuple.from(tag)).begin
                val end = tagsDir.range(Tuple.from(tag)).end

                begin -> end

              case _ =>
                throw new IllegalArgumentException(
                  "FoundationDb does not support " + offset.getClass.getSimpleName + " offsets")
            }

            val source = RangeRead
              .rangeSource(
                begin = KeySelector.firstGreaterOrEqual(begin),
                end = KeySelector.firstGreaterOrEqual(end),
                mode = StreamingMode.WANT_ALL
              )
              .via(eventsByTagFlow)
              .map { eventEnv =>
                currentOffset = eventEnv.offset
                eventEnv
              }
              .watchTermination() {
                case (currentMat, streamResult) =>
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
    } yield result

    Source.futureSource(f).mapMaterializedValue(_ => NotUsed)

  }

  private def getPersistentRepr(persistenceId: String, sequenceNr: Long): Future[Option[PersistentRepr]] = {
    for {
      keySerializer <- keySerializerFuture
      key = keySerializer.message(persistenceId, sequenceNr)
      range = key.subspace.range(key.tuple)
      result <- session.readAsync { implicit tr =>
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
    } yield result
  }

  private val eventsByTagFlow: Flow[KeyValue, EventEnvelope, NotUsed] = {
    Flow[KeyValue]
      .via(chunkAssembler)
      .mapAsync(100) {
        case AssembledPayload(key, value) =>
          for {
            keySerializer <- keySerializerFuture
            tagKey = keySerializer.tag(key)
            tag = fdbSerializer.bytes2TagType(value.toArray)
            result <- tag match {
              case CompactTag(persistenceId, sequenceNr) =>
                getPersistentRepr(persistenceId, sequenceNr).map(_.map { persistentRepr =>
                  EventEnvelope(
                    VersionstampOffset(tagKey.versionstamp),
                    persistentRepr.persistenceId,
                    persistentRepr.sequenceNr,
                    persistentRepr.payload
                  )
                }.orElse { // no persistentRepr for a given persistenceId -> seqNr, looks like the event was deleted, let's remove it from the tag too
                  session.runAsync { tr =>
                    log.info(s"no event at $persistenceId $sequenceNr, cleaning tag $tagKey")
                    tr.clear(tagKey.bytes)
                    Future.successful(Done)
                  }
                  None
                })

              case RichTag(payload) =>
                val persistentRepr = fdbSerializer.bytes2PersistentRepr(payload)
                val envelope = EventEnvelope(
                  VersionstampOffset(tagKey.versionstamp),
                  persistentRepr.persistenceId,
                  persistentRepr.sequenceNr,
                  persistentRepr.payload
                )
                Future.successful(Some(envelope))
            }
          } yield result
      }
      .mapConcat(_.toList)
  }

  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    val f = for {
      db <- session.underlying()
      keySerializer <- keySerializerFuture
      dirs <- directoriesFuture
      tagsDir = dirs.tags
      (begin, end) = offset match {
        case versionstamp: VersionstampOffset =>
          val begin = keySerializer.tag(tag, versionstamp.value).bytes
          val end = tagsDir.range(Tuple.from(tag)).end
          begin -> end

        case NoOffset =>
          val begin = tagsDir.range(Tuple.from(tag)).begin
          val end = tagsDir.range(Tuple.from(tag)).end
          begin -> end

        case _ =>
          throw new IllegalArgumentException(
            "FoundationDb does not support " + offset.getClass.getSimpleName + " offsets")
      }

    } yield
      RangeRead //since we don't use any watches here, we can just create a new long running source and read it all.
        .longRunningRangeSource(begin, end, None)(db, ec)
        .via(eventsByTagFlow)
    Source.futureSource(f).mapMaterializedValue(_ => NotUsed)
  }
}

object FoundationDbReadJournal {
  final val Identifier = "foundationdb-query-journal"
}
