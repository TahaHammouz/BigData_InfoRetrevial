import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
object InfoRetrieval {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]").
      setAppName("InfoRetrieval")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("InfoRetrieval")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val invertedIndex = sc.wholeTextFiles("./source_files")
      .flatMap {

        case (location, contents) => {
          val words = contents.split("""\W+""").filter(word => word.size > 0)
          val filename = location.split("/").last
          words.map(word => ((word.toLowerCase(), filename), 1))
        }
      }.map {
      // Create a tuple with count 1 ((word, fileName), 1)
      case (w, p) => ((w, 1))
    }.reduceByKey((n1, n2) => n1 + n2).map {
      // Transform tuple into (word, (fileName, count))
      case ((w, p), n) => (w, (p, n))
    }.groupBy {
      // Group by words
      case (w, (p, n)) => w
    }.map {
      // Output sequence of (fileName, count) into a comma seperated string
      case (w, seq) =>
        val seq2 = seq map {
          case (_, (p, n)) => (p, n)
        }
        (w, seq2.mkString(", "))
    }.sortBy { // sorting alphabetical order
      _._1
    }.saveAsTextFile("/Users/tahahammouz/Desktop/bigdata/untitled/WholeInvertedIndex")

    //taking the input from the user to search for the word
    val word = scala.io.StdIn.readLine("Enter a word to search: ")

  //create a new rdd for this file
    val data = spark.read.textFile("/Users/tahahammouz/Desktop/bigdata/untitled/WholeInvertedIndex/*").rdd
    //filtering the data to get the specific word
    val mapFile = data.filter(x => x.contains(word))
    //print each file contain the word
    mapFile.collect().foreach(println)



  }
}


