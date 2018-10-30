package com.github.uosdmlab.nkp

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{ParamMap, Params, StringArrayParam}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

private[nkp] trait TokenizerParams extends Params {

  final val filter: StringArrayParam = new StringArrayParam(this, "filter", "POS(Part Of Speech) filter")

  final def getFilter: Array[String] = $(filter)
}

/**
  * Created by jun on 2016. 10. 23..
  */
// unary란 '하나의' ( binary : 2)
// spark UnaryTransformer:
// Abstract class for transformers that take one input column, apply transformation, and output the result as a new column.
class Tokenizer(override val uid: String) extends UnaryTransformer[String, Seq[String], Tokenizer]
  with TokenizerParams with DefaultParamsWritable {

  import org.bitbucket.eunjeon.seunjeon.{Analyzer => EunjeonAnalyzer, LNode}

  def this() = this(Identifiable.randomUID("nkp_t"))

  def setFilter(value: Array[String]): this.type = set(filter, value)

  def setFilter(value: Seq[String]): this.type = setFilter(value.toArray)

  def setFilter(value: String, values: String*): this.type = setFilter(value +: values)

  setDefault(filter -> Array.empty[String])

  override protected def createTransformFunc: String => Seq[String] = { text: String =>
    val parsed: Seq[LNode] = EunjeonAnalyzer.parse(text) // Parse text using seunjeon

    val words: Seq[String] =
      if ($(filter).length == 0) parsed.map(_.morpheme.surface)
      else parsed.map { lNode: LNode =>
        val mor = lNode.morpheme // morpheme
      val poses = mor.poses.map(_.toString) intersect $(filter) // filter with POS

        if (poses.nonEmpty) Some(mor.surface) else None
      }.filter(_.nonEmpty)
        .map(_.get)

    words
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    if (Dictionary.shouldSync) {
      transformWithSync(dataset)
    }
    else {
      val transformUDF = udf(this.createTransformFunc, outputDataType)
      dataset.withColumn($(outputCol), transformUDF(dataset($(inputCol))))
    }
  }

  private final val JOIN_ID_COL: String = Identifiable.randomUID("__joinid__")

  private def transformWithSync(dataset: Dataset[_]): DataFrame = {
    val bcWords = Dictionary.broadcastWords()

    val df = dataset.withColumn(JOIN_ID_COL, monotonically_increasing_id())

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val outputDF = df.select(JOIN_ID_COL, $(inputCol))
      .mapPartitions { it: Iterator[Row] =>
        Dictionary.syncWords(bcWords)
        val tokenizeFunc = createTransformFunc
        it.map {
          case Row(joinId: Long, text: String) => (joinId, tokenizeFunc(text))
        }
      }
      .toDF(JOIN_ID_COL, $(outputCol))

    df.join(outputDF, JOIN_ID_COL).drop(JOIN_ID_COL)
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type should be String")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, true)

  override def copy(extra: ParamMap): Tokenizer = defaultCopy(extra)
}

object Tokenizer extends DefaultParamsReadable[Tokenizer] {

  override def load(path: String): Tokenizer = super.load(path)
}
