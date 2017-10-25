import java.io.{File, FileFilter}

import scala.xml.XML

object Postaggoid extends App {
  // several things here
  // we want to build a dictionary,
  // and we want to get stats
  // over words.

  // this builds a probability graph
  // and a labelling dictionary.
  import cats._
  import cats.implicits._

  val brownPath = "/home/arno/Downloads/brown_tei"
  val brownFiles = new File(brownPath).listFiles(new FileFilter {
    override def accept(file: File): Boolean = {
      file.isFile && file.getName.matches("""\w\d\d\.xml""")
    }
  })

  val allSentences: Seq[Seq[(String, String)]] = brownFiles.flatMap(x => {
    val xml = XML.loadFile(x)
    (xml \\ "s").map(s => {
      (s \\ "w").map(w => {
        w.attribute("type").get.head.text -> w.text.trim.toLowerCase
      })
    })
  })

  val (tags, dictionary) = allSentences
    .toList
    .foldMap(s => s.toList.foldMap({
      case (tag, word) => Set(tag) -> Map(word -> Set(tag))
    }))

  val frequencies = allSentences
    .filterNot(_.isEmpty)
    .map((s:Seq[(String, String)] )=> {
      "^" :: s.map(_._1).toList ++ ("$" :: Nil)
    })
    .toList
    .foldMap((s:Seq[String]) => {
      s.sliding(2).toList.foldMap(pair => pair.toList match {
        case f :: s :: Nil => Map(f -> Map(s -> 1))
        case _ => sys.error("should never happen")
      })
    })
    .map({ case (from, targets) => {
      val total = targets.toList.foldMap(_._2)

      from -> targets.map({ case (to, n) => to -> (n.toFloat / total) })
    }})

  val sentence = "The quick brown fox jumps over the lazy dog".toLowerCase().split(" ")

  val tagged = sentence.map(word =>
    dictionary.getOrElse(word, tags)
  )

  tagged.foreach( w=> {
      println(w)
  })

  val full: Seq[Set[String]] = tagged :+ Set("$")

  val annotated = full.foldLeft(Seq((List("^"), 1f))) {
    case (aggregated, current) => {

      val r = current.map(currentCat => {
        aggregated.map(previousCat => {
          (currentCat :: previousCat._1) -> frequencies.get(previousCat._1.head).flatMap(sec => sec.get(currentCat)).getOrElse(0f) * previousCat._2
        }).maxBy(_._2)
      })

      r.toSeq
    }
  }

  sentence.zip(annotated.head._1.reverse.drop(1)).foreach({ case (word, token) => println(s"${word} - ${token}")})
  println(annotated)
}
