/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 * Copyright (C) 2018 Nikita Melkozerov. <n.melkozerov at gmail dot com>
 */

package akka.persistence.cassandra.journal

import akka.actor._
import akka.persistence._
import akka.persistence.foundationdb.FoundationDbLifecycle
import akka.testkit._
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.language.postfixOps
import com.apple.foundationdb.{Database, Range => FdbRange}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

object FoundationDbLoadSpec {
  val config = ConfigFactory
    .parseString(
      s"""
      |foundationdb-journal.directory="FoundationDbLoadSpec"
      |akka.actor.serialize-messages=off
      |akka.test.single-expect-default=120s
    """.stripMargin
    )
    .withFallback(FoundationDbLifecycle.config)

  trait Measure extends { this: Actor =>
    val NanoToSecond = 1000.0 * 1000 * 1000

    var startTime: Long = 0L
    var stopTime: Long = 0L

    var startSequenceNr = 0L
    var stopSequenceNr = 0L

    def startMeasure(): Unit = {
      startSequenceNr = lastSequenceNr
      startTime = System.nanoTime
    }

    def stopMeasure(): Unit = {
      stopSequenceNr = lastSequenceNr
      stopTime = System.nanoTime
      sender ! (NanoToSecond * (stopSequenceNr - startSequenceNr) / (stopTime - startTime))
    }

    def lastSequenceNr: Long
  }

  class ProcessorA(val persistenceId: String) extends PersistentActor with Measure {
    def receiveRecover: Receive = handle

    def receiveCommand: Receive = {
      case c @ "start" =>
        deferAsync(c) { _ =>
          startMeasure(); sender ! "started"
        }
      case c @ "stop" =>
        deferAsync(c) { _ =>
          stopMeasure()
        }
      case payload: String =>
        persistAsync(payload)(handle)
    }

    def handle: Receive = {
      case _: String =>
    }
  }

  class ProcessorB(val persistenceId: String, failAt: Option[Long], receiver: ActorRef) extends PersistentActor {
    def receiveRecover: Receive = handle

    def receiveCommand: Receive = {
      case payload: String => persistAsync(payload)(handle)
    }

    def handle: Receive = {
      case payload: String =>
        receiver ! s"${payload}-${lastSequenceNr}"
    }
  }
}

import akka.persistence.cassandra.journal.FoundationDbLoadSpec._

class FoundationDbLoadSpec
    extends TestKit(ActorSystem("FoundationDbLoadSpec", config))
    with ImplicitSender
    with AnyWordSpecLike
    with Matchers
    with FoundationDbLifecycle {

  "A FoundationDb journal" should {
    "have some reasonable write throughput" in {
      val warmCycles = 5000L
      val loadCycles = 50000L

      val processor1 = system.actorOf(Props(classOf[ProcessorA], "p1a"))
      1L to warmCycles foreach { i =>
        processor1 ! "a"
      }
      processor1 ! "start"
      expectMsg("started")
      1L to loadCycles foreach { i =>
        processor1 ! "a"
      }
      processor1 ! "stop"
      expectMsgPF(100 seconds) {
        case throughput: Double =>
          println(f"\nthroughput = $throughput%.2f persistent commands per second")
      }
    }

    "work properly under load" in {
      val cycles = 10000L

      val processor1 =
        system.actorOf(Props(classOf[ProcessorB], "p1b", None, self))
      1L to cycles foreach { i =>
        processor1 ! "a"
      }
      1L to cycles foreach { i =>
        expectMsg(s"a-${i}")
      }

      val processor2 =
        system.actorOf(Props(classOf[ProcessorB], "p1b", None, self))
      1L to cycles foreach { i =>
        expectMsg(s"a-${i}")
      }

      processor2 ! "b"
      expectMsg(s"b-${cycles + 1L}")
    }
  }

}
