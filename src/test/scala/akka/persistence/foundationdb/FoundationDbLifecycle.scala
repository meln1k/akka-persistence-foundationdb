/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 * Copyright (C) 2018 Nikita Melkozerov. <n.melkozerov at gmail dot com>
 */

package akka.persistence.foundationdb

import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import akka.event.{Logging, LoggingAdapter}
import akka.persistence.PersistentActor
import akka.testkit.{TestKitBase, TestProbe}
import com.apple.foundationdb.{Cluster, Database, FDB}
import com.typesafe.config.ConfigFactory
import org.scalatest._

import scala.concurrent.duration._

object FoundationDbLifecycle {

  val config = { //todo add snapshot store
    ConfigFactory.parseString(
      s"""
    akka.persistence.journal.plugin = "foundationdb-journal"
    akka.persistence.snapshot-store.plugin = "foundationdb-snapshot-store"
    foundationdb-journal.circuit-breaker.call-timeout = 30s
    akka.test.single-expect-default = 20s
    akka.actor.serialize-messages=on
    """
    ).withFallback(ConfigFactory.load())
  }

  def awaitPersistenceInit(system: ActorSystem, journalPluginId: String = "", snapshotPluginId: String = ""): Unit = {
    val probe = TestProbe()(system)
    val t0 = System.nanoTime()
    var n = 0
    probe.within(45.seconds) {
      probe.awaitAssert {
        n += 1
        system.actorOf(Props(classOf[AwaitPersistenceInit], journalPluginId, snapshotPluginId), "persistenceInit" + n).tell("hello", probe.ref)
        probe.expectMsg(5.seconds, "hello")
        system.log.debug("awaitPersistenceInit took {} ms {}", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0), system.name)
      }
    }
  }

  class AwaitPersistenceInit(
    override val journalPluginId:  String,
    override val snapshotPluginId: String
  ) extends PersistentActor {
    def persistenceId: String = "persistenceInit"

    def receiveRecover: Receive = {
      case _ =>
    }

    def receiveCommand: Receive = {
      case msg =>
        persist(msg) { _ =>
          sender() ! msg
          context.stop(self)
        }
    }
  }
}

trait FoundationDbLifecycle extends BeforeAndAfterAll {
  this: TestKitBase with Suite =>

  lazy val db = FdbConnection.cluster.openDatabase()


  override protected def beforeAll(): Unit = {
    awaitPersistenceInit()
    super.beforeAll()
  }


  def awaitPersistenceInit(): Unit = {
    FoundationDbLifecycle.awaitPersistenceInit(system)
  }



  override protected def afterAll(): Unit = {
    foundationDbCleanup(db)
    db.close()
    shutdown(system, verifySystemShutdown = true)
    super.afterAll()
  }

  protected def foundationDbCleanup: Database => Unit = {db => }
}

object FdbConnection {
  private lazy val fdb: FDB = FDB.selectAPIVersion(510)

  lazy val cluster: Cluster = {
    val c = fdb.createCluster()
    sys.addShutdownHook {
      c.close()
      fdb.stopNetwork()
    }
    c
  }
}