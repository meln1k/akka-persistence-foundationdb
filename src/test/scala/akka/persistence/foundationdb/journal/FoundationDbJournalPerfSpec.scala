package akka.persistence.foundationdb.journal

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalPerfSpec
import akka.persistence.journal.JournalPerfSpec.ResetCounter
import akka.testkit.TestProbe
import com.apple.foundationdb.{Database, FDB}
import com.typesafe.config.ConfigFactory
import concurrent.duration._

class FoundationDbJournalPerfSpec extends JournalPerfSpec(
  config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "foundationdb-journal"
      |foundationdb-journal.tag-storing-policy = "AlwaysCompact"
      |foundationdb-journal.plugin-directory = "fdb-journal--perf-tests"
    """.stripMargin).withFallback(ConfigFactory.load())) {
  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = false

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

  protected override def afterAll(): Unit = {
    super.afterAll()
    db.close()
  }

  override def awaitDurationMillis: Long = 20000

  override def eventsCount: Int = 200


}
