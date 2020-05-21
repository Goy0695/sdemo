import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._


object tu01 {

  def main (args : Array[String]): Unit = {

    val basePath = "src/main/resources/"

    System.setProperty("hadoop.home.dir", "D:\\winutils\\hadoop-2.8.1")

    //local代表本地模式，*代表所有cpu核,可以用数字指定核数
    val spark = SparkSession.builder().
      master("local[*]").
      appName("tu01").
      config("spark.app.id","tu01").
      getOrCreate()

    //主入口，2.0以前都通过sparkContext进入
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    //本地文件生成rdd
    val input = sc.textFile(basePath + "data/kjvdat.txt").
      map(line => line.toLowerCase)

    //rdd持久化，针对频繁读取的数据性能更佳
    input.cache()

    //过滤
    val sins = input.filter(line => line.contains("sin"))
    val count = sins.count()

    // Convert the RDD into a collection (array)
    val array = sins.collect()

    //Take from Array
    //array.take(1).foreach(println)

    //Take from RDD
    //sins.take(1).foreach(println)

    //function
    val filterFunc: String => Boolean =
      (s:String) => s.contains("god") || s.contains("christ")

    //val filterFunc2: (String,String) => Boolean =
     // (s1,s2) => s1.contains("java") || s2.contains("python")

    // 自定义函数过滤
    val sinsFilter = sins.filter(filterFunc)
    // val sinsF = sins filter filterFunc
    val sinsFilterCount = sinsFilter.count
    //println(sinsFilterCount)

    //自定义方法（method）
    def peek (rdd : RDD[_], num : Int = 10): Unit = {
      println("RDD type " + rdd + "\n")
      println("*****************************")
      rdd.take(num).foreach(println)
      println("*****************************")
    }
    //peek(input)

    //flatmap : 先map然后再flat 转变为array长度与原先不一致
    //字符类\p{IsAlphabetic}匹配任何字母字符
    val patten = """[^\p{IsAlphabetic}]+"""
    val words = input.flatMap(line => line.split(patten))
    //peek(words)

    //转换成单列后就可以开始sql了
    //groupby之后得到two-element Tuples: (String, Iterable[String])
    //Tuples index from 1, rather than 0
    val wordGroups = words.groupBy(word => word)
    //peek(wordGroups)

    //方法1：从tuple根据索引取
    val wordCounts1 = wordGroups.map(line => (line._1,line._2.size))

    //方法2： case判断
    val wordCounts2 = wordGroups.map{
      case (line,group) => (line,group.size)
      case _ => (null)
    }

    //方法3：mapValues
    val wordCounts3 = wordGroups.mapValues(line => line.size)


    //方法4 : reducebykey
    val wordCounts4 = input.flatMap(line => line.split(patten)).
      map(line => (line,1)).
      reduceByKey((count1,count2) => count1 + count2)
    //peek(wordCounts4)

    val outPath = basePath+"output"
    val file=new File(outPath)


    if (file.exists()) {
      val listfiles = file.listFiles()
      listfiles.foreach(_.delete())
      file.delete()
    }
    //保存为文件
    //wordCounts4.saveAsTextFile(outPath)











  }




}
