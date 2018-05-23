package akka.persistence.foundationdb.util

import com.apple.foundationdb.directory.DirectorySubspace
import com.apple.foundationdb.tuple.Tuple

object KeySerializers {

  def tagWatchKey(tagWatchDir: DirectorySubspace, tag: String, shardId: Int) = tagWatchDir.pack(Tuple.from(tag, shardId: java.lang.Integer))

}
