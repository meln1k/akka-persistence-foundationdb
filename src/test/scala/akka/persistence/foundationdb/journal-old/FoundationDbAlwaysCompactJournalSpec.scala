package akka.persistence.foundationdb.journal

import akka.event.{Logging, LoggingAdapter}
import akka.persistence.CapabilityFlag
import akka.persistence.foundationdb.session.FoundationDbSession
import akka.persistence.foundationdb.{FoundationDbLifecycle}
import akka.persistence.foundationdb.util.RangeRead
import akka.persistence.journal.JournalSpec
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.apple.foundationdb.directory.{DirectoryLayer, DirectorySubspace}
import com.apple.foundationdb.{Database, FDB, Range => FdbRange}
import com.apple.foundationdb.tuple.Tuple
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.{Futures, ScalaFutures}

import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class FoundationDbAlwaysCompactJournalSpec extends JournalSpec(
  config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "foundationdb-journal"
      |foundationdb-journal.tag-storing-policy = "AlwaysCompact"
      |foundationdb-journal.directory = "fdb-always-compact-journal-tests"
    """.stripMargin).withFallback(FoundationDbLifecycle.config)) with ScalaFutures with FoundationDbLifecycle {

  val fdbConfig = new FoundationDbJournalConfig(system, config.getConfig("foundationdb-journal"))

  protected val log: LoggingAdapter = Logging(system, getClass)


  protected override def beforeAll(): Unit = {


    val session = new FoundationDbSession(
      system,
      fdbConfig.sessionProvider,
      log,
      fdbConfig
    )

    session.resolveDirectories().map { dirs =>
      db.run { tr =>
        tr.clear(dirs.tags.range())
        tr.clear(dirs.messages.range())
        tr.clear(dirs.maxSeqNr.range())
        tr.clear(dirs.tagWatches.range())
        tr.clear(dirs.plugin.range())
        dirs.tags.removeIfExists(db)
        dirs.messages.removeIfExists(db)
        dirs.maxSeqNr.removeIfExists(db)
        dirs.tagWatches.removeIfExists(db)
        dirs.plugin.removeIfExists(db)
      }
    }
    super.beforeAll()
  }



  override def supportsRejectingNonSerializableObjects: CapabilityFlag = false // or CapabilityFlag.off


  implicit val mat = ActorMaterializer()

//  "FoundationDb journal with AlwaysCompact tag policy" must {
//    "save fruit tags" in {
//      implicit val tcx = db
//
//      val a = system.actorOf(TestActor.props("a"))
//      a ! "hello"
////      expectMsg(20.seconds, s"hello-done")
//      a ! "a green apple"
////      expectMsg(s"a green apple-done")
//
//      Thread.sleep(200)
//      val tags = RangeRead.longRunningRangeSource(
//        journalConfig.tagsDir.range(Tuple.from("apple")),
//        None
//      )
//        .runWith(Sink.seq).futureValue
//
//      tags.size shouldEqual 1
//
//
//    }
//  }
}