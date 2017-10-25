import scala.concurrent.Future

object Main extends App {

  import smallstm._

  val a = Ref(100)
  val b = Ref(0)

  def atomic[T](thunk: TransactionExecutor[_] => T): T = {
    Engine.runTransaction(new Transaction[T](thunk))
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  val sum: Seq[Future[Int]] = (1 to 10).map(i =>
    Future {
      atomic {
        implicit executor =>
          println("i : " + i)
          //a.set(a() - 1 )
          b.set(b() + 1)
          b()
      }
    }.map(s => i -> s))
    .map(f => f.map({
      case (i, s) =>
        println(s"resultl $i : $s")
        s
    }))
  /*.map({
    case (i,s) => {

      s
    }
  })*/

  import scala.concurrent.Await
  import scala.concurrent.duration._
  import scala.concurrent._

  Await.result(Future.sequence(sum), 10.seconds)
}
