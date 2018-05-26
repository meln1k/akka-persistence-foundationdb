package akka.persistence.foundationdb.serialization

import akka.persistence.PersistentRepr
import akka.persistence.foundationdb.journal.TagStoringPolicy.{EVENT_TAG_COMPACT, EVENT_TAG_RICH}
import akka.serialization.Serialization
import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.tuple.{Tuple, Versionstamp}

class FdbSerializer(serialization: Serialization) {

  def persistentRepr2Bytes(p: PersistentRepr): Array[Byte] = serialization.serialize(p).get
  def bytes2PersistentRepr(bytes: Array[Byte]): PersistentRepr = serialization.deserialize(bytes, classOf[PersistentRepr]).get

  def persistentReprFromKeyValue(kv: KeyValue): PersistentRepr = {
    val key = Tuple.fromBytes(kv.getKey)
    key.getLong(3) match {
      case EVENT_TAG_COMPACT =>
        bytes2PersistentRepr(kv.getValue)
      case EVENT_TAG_RICH =>
        bytes2PersistentRepr(kv.getValue.drop(Versionstamp.LENGTH))
    }
  }

}
