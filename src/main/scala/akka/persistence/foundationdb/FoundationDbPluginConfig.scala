package akka.persistence.foundationdb


import com.apple.foundationdb.FDB
import com.apple.foundationdb.directory.DirectoryLayer
import com.typesafe.config.Config
import scala.collection.JavaConverters._

class FoundationDbPluginConfig(config: Config) {


  private val fdb = FDB.selectAPIVersion(510)

  implicit val db = fdb.open()

  protected val directoryLayer = new DirectoryLayer()

  val pluginDirectory = directoryLayer.createOrOpen(db, List(config.getString("plugin-directory")).asJava).get()

  val tagsDir = pluginDirectory.createOrOpen(db, List("tags").asJava).get()
  val eventLogDir = pluginDirectory.createOrOpen(db, List("logs").asJava).get()
  val seqNoDir = pluginDirectory.createOrOpen(db, List("seqNo").asJava).get()
  val tagWatchDir = pluginDirectory.createOrOpen(db, List("tagWatches").asJava).get()


  val TagWatchShards: Map[String, Int] = config
    .getObject("tag-watch-shards")
    .asScala
    .map { case (tagName, shards) =>
      val nrOfShards = shards.unwrapped().asInstanceOf[java.lang.Integer].toInt
      require(nrOfShards > 0, "number of shards must be positive")
      tagName -> nrOfShards
    }.toMap


}
