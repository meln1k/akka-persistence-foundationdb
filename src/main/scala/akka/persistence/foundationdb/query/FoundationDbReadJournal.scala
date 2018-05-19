package akka.persistence.foundationdb.query

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.persistence.query.{EventEnvelope, Offset}
import akka.persistence.query.scaladsl._
import akka.stream.scaladsl.Source

class FoundationDbReadJournal(system: ExtendedActorSystem)
  extends ReadJournal
    with PersistenceIdsQuery
    with CurrentPersistenceIdsQuery
    with EventsByPersistenceIdQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByTagQuery
    with CurrentEventsByTagQuery {




  override def persistenceIds(): Source[String, NotUsed] = ???

  override def currentPersistenceIds(): Source[String, NotUsed] = ???

  override def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = ???

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = ???

  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = ???

  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = ???
}
