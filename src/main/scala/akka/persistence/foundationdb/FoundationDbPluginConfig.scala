package akka.persistence.foundationdb


import com.apple.foundationdb.FDB
import com.apple.foundationdb.directory.DirectoryLayer
import com.typesafe.config.Config
import scala.collection.JavaConverters._

class FoundationDbPluginConfig(config: Config) {


  private val fdb = FDB.selectAPIVersion(510)

  implicit val db = fdb.open()

  private val directoryLayer = new DirectoryLayer()

  protected val pluginDirectory = directoryLayer.createOrOpen(db, List("fdb-journal").asJava).get()

  val tagsDir = pluginDirectory.createOrOpen(db, List("tags").asJava).get()
  val logsDir = pluginDirectory.createOrOpen(db, List("logs").asJava).get()
  val seqNoDir = pluginDirectory.createOrOpen(db, List("seqNo").asJava).get()
  val tagWatchDir = pluginDirectory.createOrOpen(db, List("tagWatches").asJava).get()



}
