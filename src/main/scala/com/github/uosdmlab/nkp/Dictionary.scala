package com.github.uosdmlab.nkp

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.bitbucket.eunjeon.seunjeon.{Analyzer => EunjeonAnalyzer}

// 스파크 SQL : 구조화된 데이터셋을 간단하고 효율적으로 다루는 수단을 제공
// * 데이터셋으로부터 조건에 맞는 데이터 추출
// * JSON의 키와 테이블의 컬럼 등, 특정한 이름으로 데이터 추출
// * 복수의 데이터셋 결합
// * 그룹 단위로 집약
// * 다른 형식의 구조화된 데이터셋으로 


object Dictionary {

  // Words inside driver. This won't be modified in executor.
  // private[packagename] 같은 패키지 아래 있는 애들은 이걸 공통으로 쓸 수 있다.
  private[nkp] var words = Seq.empty[String]

  /**
    * Executed from driver.
    */
  private[nkp] def broadcastWords(): Broadcast[Seq[String]] = {
    SparkSession.builder().getOrCreate().sparkContext.broadcast(words)
  }

  /**
    * Executed from executors.
    * NOTE: broadcastWords() should be executed first.
    */
  private[nkp] def syncWords(bcWords: Broadcast[Seq[String]]): Unit = {
    EunjeonAnalyzer.resetUserDict()
    EunjeonAnalyzer.setUserDict(bcWords.value.iterator)
  }

  def reset(): this.type = chain {
    words = Seq.empty[String]
  }

  private var isDictionaryUsed = false

  private[nkp] def shouldSync = {
    isDictionaryUsed
  }

  // String*, *붙은 이유
  // method parameter that can take a variable number of arguments , i.e , a varargs field
  // String* : Sequence of String
  def addWords(word: String, words: String*): this.type = addWords(word +: words)

  def addWords(words: Traversable[String]): this.type = chain {
    this.words = this.words ++ words
    isDictionaryUsed = true
  }

  def addWordsFromCSV(path: String, paths: String*): this.type = addWordsFromCSV(path +: paths)

  def addWordsFromCSV(paths: Traversable[String]): this.type = chain {
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    val schema = StructType(Array(
      StructField("word", StringType, nullable = false),
      StructField("cost", StringType, nullable = true)))

    val df = spark.read
      .option("sep", ",")
      .option("inferSchema", value = false)
      .option("header", value = false)
      .schema(schema)
      .csv(paths.toSeq: _*)

    val words = df.map {
      case Row(word: String, cost: String) =>
        s"$word,$cost"
      case Row(word: String, null) =>
        word
    }.collect()

    addWords(words)
  }

  private def chain(fn: => Any): this.type = {
    fn
    this
  }
}
