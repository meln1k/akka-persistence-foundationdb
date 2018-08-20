package akka.persistence.foundationdb.serialization

import com.apple.foundationdb.tuple.Tuple

case class SerializedMessage(
    persistenceId: String,
    sequenceNr: Long,
    payload: Array[Byte]
) {
  def keyTuple = Tuple.from(persistenceId, sequenceNr: java.lang.Long)
}
