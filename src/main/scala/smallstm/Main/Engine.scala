package smallstm

import java.util.concurrent.ConcurrentHashMap

// We exclude the case in which refs are created when running the
// futures (register cannot be called when runTransaction is)
object Engine {
  //  val refs = new ConcurrentHashMap[Ref, Int]

  // do not use lock use synchronized
  //  private val transactionLock: Lock = new Object()
  private val runningTransactions = new ConcurrentHashMap[Transaction[_], Unit]()

  private[smallstm] var refs = Map.empty[Ref,Int]

  // we need a structure to check what is ongoing
  def register(r: Ref, v: Int) = {
    this.synchronized {
      refs = refs + (r -> v)
    }
  }

  def runTransaction[T](t: Transaction[T]) : T = {
    import scala.collection.JavaConversions._

    var done = false
    var result: Option[T] = None

    while (!done){
      // the values as they were when started
      val executor = TransactionExecutor(t, refs)
      runningTransactions.put(t, ())

      // non concurrency limit in transactions,
      // just a concurrency limit in commit
      result = Some(t.thunk(executor))

      // try to commit the transaction
      this.synchronized {
        // everything I read is coherent,
        // so my decisions are supposedly okay
        val valid = executor.values
          .filter(e => e._2._1 == Read)
          .forall({
            case (ref, _) => refs
              .get(ref) == executor.snapshot.get(ref)
          })

        if (valid) {
          println("valid")
          runningTransactions.remove(t)

          refs = executor.values.foldLeft(refs)({
            case (a, c) => a + (c._1 -> c._2._2)
          })

          // refs.putAll(executor.transactionValues)
          done = true
        } else {
          println("invalid, retrying")
        }
      }
    }

    result.get
  }
}
