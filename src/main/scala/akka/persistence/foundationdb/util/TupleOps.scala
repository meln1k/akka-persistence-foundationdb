package akka.persistence.foundationdb.util

import com.apple.foundationdb.tuple.Tuple

object TupleOps {

  def persistentReprId2Tuple(persistenceId: String, sequenceNr: Long): Tuple = {
    Tuple.from(persistenceId, sequenceNr: java.lang.Long)
  }

}
