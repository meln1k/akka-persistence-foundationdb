package akka.persistence.foundationdb.journal

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.{Database, FDB, Range => FdbRange}
import com.apple.foundationdb.tuple.Tuple
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._

class FoundationDbJournalSpec extends JournalSpec(
  config = ConfigFactory.load()) {

  var db: Database = _

  protected override def beforeAll(): Unit = {
    val fdb = FDB.selectAPIVersion(510)

    db = fdb.open()

    val directoryLayer = new DirectoryLayer()

    val pluginDirectory = directoryLayer.createOrOpen(db, List("fdb-journal-test").asJava).get()

    val tagDir = pluginDirectory.createOrOpen(db, List("tag").asJava).get()
    val logDir = pluginDirectory.createOrOpen(db, List("log").asJava).get()
    val seqNoDir = pluginDirectory.createOrOpen(db, List("seqNo").asJava).get()


    db.run { tr =>
      tr.clear(tagDir.range())
      tr.clear(logDir.range())
      tr.clear(seqNoDir.range())
      tr.clear(pluginDirectory.range())
      tagDir.removeIfExists(db)
      logDir.removeIfExists(db)
      seqNoDir.removeIfExists(db)
      pluginDirectory.removeIfExists(db)
    }


    super.beforeAll()


  }


  protected override def beforeEach(): Unit = {
    super.beforeEach()
  }

  protected override def afterAll(): Unit = {
    db.close()
    super.afterAll()
  }

  override def supportsRejectingNonSerializableObjects: CapabilityFlag = false // or CapabilityFlag.off
}