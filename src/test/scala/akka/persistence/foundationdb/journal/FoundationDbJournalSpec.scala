package akka.persistence.foundationdb.journal

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.apple.foundationdb.directory.{DirectoryLayer, DirectorySubspace}
import com.apple.foundationdb.{Database, FDB, Range => FdbRange}
import com.apple.foundationdb.tuple.Tuple
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._

class FoundationDbJournalSpec extends JournalSpec(
  config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "foundationdb-journal"
      |foundationdb-journal.tag-storing-policy = "AlwaysCompact"
      |foundationdb-journal.plugin-directory = "fdb-journal-tests"
    """.stripMargin).withFallback(ConfigFactory.load())) {

  var db: Database = _

  protected override def beforeAll(): Unit = {
    val fdb = FDB.selectAPIVersion(510)

    db = fdb.open()

    val journalConfig = new FoundationDbJournalConfig(config.getConfig("foundationdb-journal"))


    db.run { tr =>
      tr.clear(journalConfig.tagsDir.range())
      tr.clear(journalConfig.eventLogDir.range())
      tr.clear(journalConfig.seqNoDir.range())
      tr.clear(journalConfig.tagWatchDir.range())
      tr.clear(journalConfig.pluginDirectory.range())
      journalConfig.tagsDir.removeIfExists(db)
      journalConfig.eventLogDir.removeIfExists(db)
      journalConfig.seqNoDir.removeIfExists(db)
      journalConfig.tagWatchDir.removeIfExists(db)
      journalConfig.pluginDirectory.removeIfExists(db)
    }


    super.beforeAll()


  }


  protected override def beforeEach(): Unit = {
    super.beforeEach()
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    db.close()
  }

  override def supportsRejectingNonSerializableObjects: CapabilityFlag = false // or CapabilityFlag.off
}