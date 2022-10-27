import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object Problem1 {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)

    // Filter stop words from array of strings.
    def filterStop(arr : Array[String], filterWords : Array[String]) : Array[String] = {
      val result = new ArrayBuffer[(String)]
      for (str <- arr){
        if (!(filterWords contains str) && str.nonEmpty && str.charAt(0).isLetter) {
          result += str.toLowerCase()
        }
      }
      result.toArray
    }

    /* Map key-value pairs as co-occurances of words in a string array. Co-occurances (w, u) and (u, w) are
    treated equally and as such co-occuring terms are sorted. */
    def mapStrings(arr : Array[String]) : ListBuffer[(String, Int)] = {
      val result = new ListBuffer[(String, Int)]
      for (i <- arr.indices){
        for (j <- i + 1 until arr.length){
          if (arr(i) < arr(j)) result += Tuple2(arr(i) + "," + arr(j), 1) else result += Tuple2(arr(j) + "," + arr(i), 1)
        }
      }
      result
    }

    // Load files and map stop-words.
    val fileRDD = sc.textFile(args(2))
    val stopWordsRDD = sc.textFile(args(1))
    val stopWords = stopWordsRDD.flatMap(_.split(" ")).collect

    //  Filter, map and reduce text file. Sorting of results done with reduction.
    val filtered = fileRDD.map(x => x.subSequence(9,x.length).toString).map(_.split(" ")).map(x => filterStop(x,stopWords))
    val mapped = filtered.flatMap(x => mapStrings(x))
    val reduced = mapped.reduceByKey(_+_).sortBy(_._1).map(_.swap).sortByKey(false).take(args(0).toInt).map(_.swap)

    // Parse output and save to file.
    val result = reduced.map(x => x._1 + "\t" + x._2.toString)
    sc.parallelize(result).saveAsTextFile(args(3))

    sc.stop
  }
}
