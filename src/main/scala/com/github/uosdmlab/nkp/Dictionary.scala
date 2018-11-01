package com.github.uosdmlab.nkp

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.bitbucket.eunjeon.seunjeon.{Analyzer => EunjeonAnalyzer}

// 패키지(packages) import
// 와일드카드(wildcard) import scala.collection._ : _가 모든 걸 포함함
// 선택적인(selective)  import scala.collection.{Vector, Sequence} array처럼 import도 가능함
// 패키지명 재정의(renaming) import scala.collection.{Vector => Vec28} 
// 제외 import java.util.{Date => _, _} Data 관련 패키지를 제외한 java.util 패키지를 전부 import
// 패키지 선언(declare) package pkg at start of file 
//                     package pkg { ... }

// 브로드캐스트 
// 복수의 이그제큐터가 동일한 변수를 참조할 수 있도록 하여 효율적으로 처리하는 
// 브로드캐스트가 아닌 일반 변수인 경우 매번 할당된 태스크의 개수와 같은 횟수만큼 동일한 데이터를 배포하여 비효율적이다.

// 스파크 SQL : 구조화된 데이터셋을 간단하고 효율적으로 다루는 수단을 제공
// * 데이터셋으로부터 조건에 맞는 데이터 추출
// * JSON의 키와 테이블의 컬럼 등, 특정한 이름으로 데이터 추출
// * 복수의 데이터셋 결합
// * 그룹 단위로 집약
// * 다른 형식의 구조화된 데이터셋으로 
// 스파크 SQL은 드라이버 프로그램에서 다양한 형식의 데이터셋을 하나로 다루고자 DataFrame이라는 추상적인 자료구조를 이용함
// DataFrame은 RDBMS의 테이블처럼 행(row), 이름, 자료형이 부여되는 컬럼(column)의 개념을 가지는 자료구조
// 쿼리 언어로서 기존에 SQL 사용자들이 자연스럽게 받아들일 수 있음

object Dictionary {

  // Words inside driver. This won't be modified in executor.
  // private[packagename] 같은 패키지 아래 있는 애들은 이걸 공통으로 쓸 수 있다.
  // collection에 추가할 수 있도록 미리 빈 공간을 만들어놓는다.
  private[nkp] var words = Seq.empty[String]

  /**
    * Executed from driver.(브로드캐스트는 드라이버 프로그램에서 실행된다)
    */
  private[nkp] def broadcastWords(): Broadcast[Seq[String]] = {
    SparkSession.builder().getOrCreate().sparkContext.broadcast(words)
  }

  /**
    * Executed from executors.
    * NOTE: broadcastWords() should be executed first. 
    * Unit은 자바 등에서의 void와 유사함. '=' 기호가 빠진 def 선언이라면 해당 def는 Unit을 반환함
    */
  private[nkp] def syncWords(bcWords: Broadcast[Seq[String]]): Unit = {
    EunjeonAnalyzer.resetUserDict()
    EunjeonAnalyzer.setUserDict(bcWords.value.iterator)
  }

  
  // ???? chain이 뭐지?????
  def reset(): this.type = chain {
    words = Seq.empty[String]
  }

  // Dictionary가 사용되었는지 알기 위한 변수
  private var isDictionaryUsed = false

  // isDictionaryUsed를 반환, true/false 값을 가지며
  // 사용된 Dictionary의 경우 true를 반환(Sync를 맞춰야 한다)
  private[nkp] def shouldSync = {
    isDictionaryUsed
  }

  // String*, *붙은 이유
  // method parameter that can take a variable number of arguments , i.e , a varargs field
  // String* : Sequence of String
  // +=는 words에 word를 concatenate한다.
  def addWords(word: String, words: String*): this.type = addWords(word +: words)

  // Traversable(방문가능) 
  // word를 추가한 후 isDictionaryUsed를 true로 바꿔줌(syncDictionary 메서드 실행 위해???)
  def addWords(words: Traversable[String]): this.type = chain {
    this.words = this.words ++ words
    isDictionaryUsed = true
  }

  // spark SQL을 사용해서 csv에서부터 word를 더해주는 것 같다???
  // paths: String* 타입으로 보아 여러 개의 CSV 경로를 한꺼번에 넣어줘도 처리가 가능한 듯 하다.
  // 오버로딩을 사용하여 매개변수가 달라도 같은 함수이름으로 처리할 수 있게 해주었다.
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
