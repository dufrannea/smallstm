package smallstm

class Ref private(value: Int) {
  def set(value:Int)(implicit transactionExecutor: TransactionExecutor[_]) : Unit = {
    transactionExecutor.write(this, value)
  }
  private def get()(transactionExecutor: TransactionExecutor[_]) : Int = {
    transactionExecutor.read(this)
  }

  def apply()(implicit transactionExecutor: TransactionExecutor[_]) =
    get()(transactionExecutor)
}

object Ref {
  def apply(value: Int): Ref =
  {
    val r = new Ref(value)
    Engine.register(r, value)
    r
  }
}