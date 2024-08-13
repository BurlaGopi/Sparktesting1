import org.apache.spark.sql.SparkSession

object newproject {

  def main(arrgs:Array[String]):Unit={
    val v=39
    println(v)

    val spark=SparkSession.builder().appName("newapp").master("local").getOrCreate()

    val df=spark.read.format("csv").option("header","true").option("delimiter",",").load("C:\\Users\\HP\\Desktop\\new_file.txt")
  df.show()
    df.printSchema()
  }
}
