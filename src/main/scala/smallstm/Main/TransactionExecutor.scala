package smallstm

sealed trait Operation
case object Read  extends Operation
case object Write extends Operation

// We should track all values that have
// been read before being written
case class TransactionExecutor[T](transaction: Transaction[T], snapshot: Map[Ref, Int]) {

  private[smallstm] val values = scala.collection.mutable.Map.empty[Ref, (Operation, Int)]

  // keep track of values you read
  var readValues = List.empty[(Ref, Int)]

  // keep track of values you wrote
  var writtenValues = List.empty[(Ref,Int)]

  def run(): T = transaction.thunk(this)

  def read(ref: Ref) = {
    this.synchronized {
      if (!values.contains(ref)) {
        // consider all values are in the global state snapshot
        values.put(ref, Read -> snapshot.get(ref).get)
      }
      values.get(ref).get._2
    }
  }

  def write(ref: Ref, value: Int) =
    this.synchronized {
      values.get(ref) match {
        case None => values.put(ref, Write -> value)
        case Some((Write, _)) => values.put(ref, Write -> value)
        case Some((Read, _)) => values.put(ref, Read -> value)
      }
    }

}

case class Transaction[T](thunk: TransactionExecutor[_] => T)



