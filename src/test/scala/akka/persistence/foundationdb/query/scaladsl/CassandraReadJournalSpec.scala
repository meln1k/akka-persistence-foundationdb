/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 * Copyright (C) 2018 Nikita Melkozerov <http://www.lightbend.com>
 */

package akka.persistence.foundationdb.query.scaladsl

import java.time.Instant
import scala.concurrent.duration._
import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import akka.persistence.foundationdb.FoundationDbLifecycle
import akka.persistence.foundationdb.query.scaladsl.FoundationDbReadJournal
import akka.persistence.foundationdb.query.TestActor
import akka.persistence.foundationdb.query.FoundationDbReadJournalConfig
import akka.persistence.journal.{Tagged, WriteEventAdapter}
import akka.persistence.query.PersistenceQuery
import akka.stream.testkit.scaladsl.TestSink
import akka.persistence.query.NoOffset
import com.apple.foundationdb.Database
import com.apple.foundationdb.{Range => FdbRange}
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.Tuple
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.JavaConverters._

object FoundationDbReadJournalSpec {

  val config = ConfigFactory
    .parseString( //todo fix the directory in the read journal
      s"""

    foundationdb-journal.directory="ScalaFoundationDbReadJournalSpec"

    akka.loglevel = INFO
    akka.actor.serialize-messages=off

    foundationdb-journal.event-adapters {
      test-tagger = akka.persistence.foundationdb.query.scaladsl.TestTagger
    }
    foundationdb-journal.event-adapter-bindings = {
      "java.lang.String" = test-tagger
    }
    cassandra-journal.log-queries = off
    """)
    .withFallback(FoundationDbLifecycle.config)
}

class TestTagger extends WriteEventAdapter {
  override def manifest(event: Any): String = ""
  override def toJournal(event: Any): Any = event match {
    case s: String if s.startsWith("a") =>
      Tagged(event, Set("a"))
    case _ =>
      event
  }
}

class FoundationDbReadJournalSpec
    extends TestKit(ActorSystem("ScalaFoundationDbReadJournalSpec", FoundationDbReadJournalSpec.config))
    with ImplicitSender
    with AnyWordSpecLike
    with FoundationDbLifecycle
    with Matchers {

  implicit val mat = ActorMaterializer()(system)

  lazy val queries: FoundationDbReadJournal =
    PersistenceQuery(system).readJournalFor[FoundationDbReadJournal](FoundationDbReadJournal.Identifier)

  "Cassandra Read Journal Scala API" must {
    "start eventsByPersistenceId query" in {
      val a = system.actorOf(Props(new TestActor("a")))
      a ! "a-1"
      expectMsg("a-1-done")

      val src = queries.eventsByPersistenceId("a", 0L, Long.MaxValue)
      src
        .map(_.persistenceId)
        .runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("a")
        .cancel()
    }

    "start current eventsByPersistenceId query" in {
      val a = system.actorOf(Props(new TestActor("b")))
      a ! "b-1"
      expectMsg("b-1-done")

      val src = queries.currentEventsByPersistenceId("b", 0L, Long.MaxValue)
      src
        .map(_.persistenceId)
        .runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("b")
        .expectComplete()
    }

    // these tests rely on events written in previous tests
    "start eventsByTag query" in {
      val src = queries.eventsByTag("a", NoOffset)
      src
        .map(_.persistenceId)
        .runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("a")
        .expectNoMessage(100.millis)
        .cancel()
    }

    "start current eventsByTag query" in {
      val src = queries.currentEventsByTag("a", NoOffset)
      src
        .map(_.persistenceId)
        .runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("a")
        .expectComplete()
    }

//    "insert Cassandra metrics to Cassandra Metrics Registry" in {
//      val registry = CassandraMetricsRegistry(system).getRegistry
//      val snapshots = registry.getNames.toArray().filter(value => value.toString.startsWith(s"${CassandraReadJournal.Identifier}"))
//      snapshots.length should be > 0
//    }
  }
}
