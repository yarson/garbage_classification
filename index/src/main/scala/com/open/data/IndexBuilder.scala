package com.open.data

import org.apache.spark.sql.{SaveMode, SparkSession, functions}

/**
  *  垃圾分类数据正/倒排索引spark构建程序.
  */
object IndexBuilder {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      throw new Exception("'inputPath', 'forwardIndexPath', 'invertIndexPath' is need!\n" +
        "Usage: ... <mainClass> inputPath=<path> forwardIndexPath=<path> invertIndexPath=<path>")
    }
    // 参数解析及完整性校验
    val argsMap = argsParse(args)
    if (!argsCheck(argsMap)) {
      throw new Exception("Usage: ... <mainClass> inputPath=<path> forwardIndexPath=<path> invertIndexPath=<path>")
    }

    /**
      * 四种垃圾分类文件所在目录.
      *
      * 数据结构：`中文名 \t 英文名 \t 分类 \t 描述`
      */
    val inputPath = args(0)
    /**
      * 正排索引保存路径.
      */
    val forwardIndexPath = args(1)
    /**
      * 倒排索引保存路径.
      */
    val invertIndexPath = args(2)

    val spark = SparkSession
      .builder()
      .appName("GarbageClassification")
      .getOrCreate()

    import spark.implicits._
    // build forward index
    val forwardDF = spark.read.text(inputPath)
      .mapPartitions(rows => { //1.解析垃圾分类数据
        val result = scala.collection.mutable.ArrayBuffer.empty[Tuple4[String, String, String, String]]
        while (rows.hasNext) {
          val row = rows.next()
          val value = row.getAs[String]("value")
          val fields = value.split("\t", -1)
          if (fields.length >= 4) {
            val chineseName = fields(0).trim
            val englishName = fields(1).trim
            val garbageType = fields(2).trim
            val describe = fields(3).trim
            result += Tuple4(chineseName, englishName, garbageType, describe)
          }
        }
        result.toArray.iterator
      })
      .toDF("chineseName", "englishName", "garbageType", "describe")
      .dropDuplicates("chineseName") //根据中文名去重
      .withColumn("id", functions.monotonically_increasing_id()) // 添加docId
      .persist()

    println(s"Forward index count: ${forwardDF.count()}")
    //TODO: delete forward/invert index path if exist
    // 保存正排数据到指定路径，用于搜索引擎
    forwardDF.write.mode(SaveMode.Overwrite).text(forwardIndexPath)
    println(s"Save forward index data into: ${forwardIndexPath} success.")
    // 构建倒排索引并保存到指定路径，用于搜索引擎
    val invertIndexCounter = spark.sparkContext.longAccumulator("invertIndexCounter")
    forwardDF.select("id", "chineseName")
      .flatMap(row => {
        val result = scala.collection.mutable.ArrayBuffer.empty[Tuple3[String, Long, Float]]
        val id = row.getAs[Long]("id")
        val chineseName = row.getAs[String]("chineseName")
        //TODO: 垃圾名称分词
        wordSegmentation(chineseName).foreach(word => result += Tuple3(word._1, id, word._2))
        result.toArray.iterator
      })
      .groupByKey(tuple => tuple._1)
      .mapGroups((key, tuples) => {
        val docList = scala.collection.mutable.ArrayBuffer.empty[String]
        while(tuples.hasNext) {
          val tuple = tuples.next()
          docList += s"${tuple._2}:${tuple._3}" // docId:weight
        }
        (key, docList.toArray.mkString(",")) // key -> docId:weight[,docId:weight]
      })
      .mapPartitions(tuples => {
        val result = scala.collection.mutable.ArrayBuffer.empty[String]
        while(tuples.hasNext) {
          invertIndexCounter.add(1L)
          val tuple = tuples.next()
          result += s"${tuple._1}\1${tuple._2}" // key \1 docList
        }
        result.toArray.iterator
      })
      .write.mode(SaveMode.Overwrite).text(invertIndexPath)
    println(s"Save invert index data into: ${invertIndexPath} success.")
    println(s"Invert index count: ${invertIndexCounter.value}")
  }

  /**
    * 程序启动参数解析
    * @param args 参数数组
    * @return key-value格式的参数
    */
  def argsParse(args: Array[String]): Map[String, String] = {
    val result = scala.collection.mutable.Map.empty[String, String]
    args.foreach(line => {
      val kv = line.split("=", -1)
      if (kv.length == 2) {
        result += (kv(0).trim -> kv(1).trim)
      }
    })
    result.toMap
  }

  /**
    * 参数完整性校验
    * @param args 参数集合
    * @return 参数完整返回true，否则返回false
    */
  def argsCheck(args: Map[String, String]): Boolean = {
    if (!args.contains("inputPath")
      || !args.contains("forwardIndexPath")
      || !args.contains("invertIndexPath")) false
    else true
  }
  /**
    * 对垃圾名称分词.
    *
    * <p>由于垃圾名较短，且多为专有名词，故优先使用前缀分词，还可以定义其他分词方式。</p>
    * @param data 垃圾名称
    * @return 分词后的结果: List(word, weight)
    */
  def wordSegmentation(data: String): List[Tuple2[String, Float]] = {
    // 前缀分词
    prefixSegmentation(data)
  }

  /**
    * 前缀分词
    * @param data 待分词字符串
    * @return 前缀分词结果: List(word, weight)
    */
  def prefixSegmentation(data: String): List[Tuple2[String, Float]] = {
    val result = scala.collection.mutable.ListBuffer.empty[Tuple2[String, Float]]
    for (index <- data.indices) {
      result += Tuple2(data.substring(0, index+1), (index+1)/data.length.toFloat)
    }
    result.toList
  }
}
