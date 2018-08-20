/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 * Copyright (C) 2018 Nikita Melkozerov. <n.melkozerov at gmail dot com>
 */

package akka.persistence.foundationdb.journal

import akka.actor.Actor
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.persistence.JournalProtocol.{ReplayMessages, WriteMessageFailure, WriteMessages, WriteMessagesFailed}

import scala.concurrent.duration._
import akka.persistence.journal._
import akka.persistence.foundationdb.FoundationDbLifecycle
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory

object FoundationDbJournalConfiguration {
  lazy val config = ConfigFactory
    .parseString(
      s"""
      |foundationdb-journal.directory = "FoundationDbJournalSpec"
    """.stripMargin
    )
    .withFallback(FoundationDbLifecycle.config)

  lazy val perfConfig = ConfigFactory
    .parseString(
      """
    akka.actor.serialize-messages=off
    foundationdb-journal.directory=FoundationDbJournalPerfSpec
    foundationdb-snapshot-store.directory=FoundationDbJournalPerfSpecSnapshot
    """
    )
    .withFallback(config)
}

class FoundationDbJournalSpec extends JournalSpec(FoundationDbJournalConfiguration.config) with FoundationDbLifecycle {

  override def supportsRejectingNonSerializableObjects = false
//  "A Cassandra Journal" must {
//
//    "be able to replay messages after serialization failure" in {
//      // there is no chance that a journal could create a data representation for type of event
//      val notSerializableEvent = new Object {
//        override def toString = "not serializable"
//      }
//      val msg = PersistentRepr(
//        payload = notSerializableEvent,
//        sequenceNr = 6,
//        persistenceId = pid,
//        sender = Actor.noSender,
//        writerUuid = writerUuid
//      )
//
//      val probe = TestProbe()
//
//      journal ! WriteMessages(List(AtomicWrite(msg)), probe.ref, actorInstanceId)
//      val err = probe.expectMsgPF() {
//        case WriteMessagesFailed(cause) => cause
//      }
//      probe.expectMsg(WriteMessageFailure(msg, err, actorInstanceId))
//
//      journal ! ReplayMessages(5, 5, 1, pid, probe.ref)
//      probe.expectMsg(replayedMessage(5))
//    }
//  }
}

class FoundationDbJournalPerfSpec
    extends JournalPerfSpec(FoundationDbJournalConfiguration.perfConfig)
    with FoundationDbLifecycle {

  override def eventsCount: Int = 100

  override def awaitDurationMillis: Long = 20.seconds.toMillis

  override def supportsRejectingNonSerializableObjects = false
}
