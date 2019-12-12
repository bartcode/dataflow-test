package example

import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.{SCollection, SideInput, WindowOptions}
import com.typesafe.config.{Config, ConfigFactory}
import example.proto.message.NumberBuffer
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner.LATEST
import org.apache.beam.sdk.transforms.windowing.{AfterPane, IntervalWindow, Repeatedly}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.{DateTime, Duration, Instant}

import scala.collection.JavaConverters._


class SourceMerge(@transient val sc: ScioContext) extends Serializable {

  import example.SourceMerge.{NumberInfoRow, NumberResults}

  /**
   * Process Pub/Sub source and BigQuery table.
   *
   * @param inputSubscription : Path to subscription.
   * @param numberInfo        : BigQuery table reference.
   */
  // noinspection ScalaStyle
  def processSources(inputSubscription: String, numberInfo: String, resultInfoTable: String): Unit = {
    val windowedInput = getInputStream(inputSubscription)
      .transform("Timestamp")(
        _.timestampBy(x =>
          new DateTime(x.timestamp).toInstant, allowedTimestampSkew = Duration.standardSeconds(5)))
      .transform("Apply window functions")(
        _.withFixedWindows(Duration.standardSeconds(5),
          options = WindowOptions(
            timestampCombiner = LATEST,
            accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
            trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
            allowedLateness = Duration.standardSeconds(5)))
          .map(_.number)
          .withWindow[IntervalWindow]
          .map { case (score, window) => (window.end(), score) })

    val sumCounts = windowedInput
      .transform("Determine counted values")(_.countByKey)

    val numberInfoSideInput = getNumberInfoSideInput

    windowedInput
      .transform("Sum values")(
        _.sumByKey)
      .transform("Extract values")(
        _.map(x => (x._1, x._2))) // Number length, (timestamp, sum)
      .withSideInputs(numberInfoSideInput) // Since the right side is small, a side input can be used.
      .map {
        case (score, side) =>
          val numberInfoMap = side(numberInfoSideInput)
          (score,
            numberInfoMap.filter {
              case (lowerBound: Long, upperBound: Long, _: String) =>
                (lowerBound <= score._2) && (upperBound >= score._2)
            }
              .map(x => x._3) // The number type: "hundreds", "thousands", etc.
              .head)
      }
      .toSCollection
      .transform("Combine with counts")(
        _.map { case ((timestamp, numberSum), numberType) => (timestamp, (numberSum, numberType)) }
          .hashLeftJoin(sumCounts))
      .transform("Tuple values")(
        _.map { case (timestamp, ((numberSum, numberType), numberCount)) =>
          (timestamp, numberSum, numberType, numberCount.getOrElse(0.toLong))
        })
      .transform("Convert into TableRow")(
        _.map {
          case (timestamp, numberSum, numberType, numberCount) => TableRow(Map(
            "update_time" -> Timestamp(timestamp),
            "number_sum" -> numberSum,
            "number_count" -> numberCount,
            "number_type" -> numberType,
            "version" -> "run-1")
            .map(kv => (kv._1, kv._2))
            .toList: _*)
        })
      .saveAsCustomOutput("Save aggregate to BQ",
        BigQueryIO
          .writeTableRows()
          .to(resultInfoTable)
          .withSchema(BigQueryType[NumberResults].schema)
          .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
          .withWriteDisposition(WriteDisposition.WRITE_APPEND))
  }

  /**
   * Load parsed stream of messages from Google Cloud Pub/Sub.
   *
   * @param inputSubscription : Path to subscription.
   * @return SCollection of parsed messages.
   */
  def getInputStream(inputSubscription: String): SCollection[NumberBuffer] = sc
    .pubsubSubscription[Array[Byte]](inputSubscription)
    .transform("Parse objects")(
      _.map(NumberBuffer.parseFrom))

  /**
   * Load typed BigQuery table which contains information about the numbers.
   *
   * @return Side input
   */
  def getNumberInfoSideInput: SideInput[Seq[(Long, Long, String)]] = sc.typedBigQuery[NumberInfoRow]()
    .map(x => (x.lower_bound.get, x.upper_bound.get, x.number_type.get))
    .asListSideInput
}

object SourceMerge {
  val config: Config = ConfigFactory.load()
  val projectId: String = config.getString("pipeline.project_id")
  val bucketPath: String = config.getString("pipeline.bucket")
  val inputSubscription: String = config.getString("pipeline.input_subscription")
  val region: String = config.getString("pipeline.region")
  val numberInfo: String = config.getString("pipeline.number_info_table")
  val resultInfoTable: String = config.getString("pipeline.result_table")

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, _) = ContextAndArgs(cmdlineArgs ++ Array(
      s"--stagingLocation=$bucketPath/staging",
      s"--tempLocation=$bucketPath/temp/"))

    sc.options.setJobName("example-etl")
    sc.optionsAs[DataflowPipelineOptions].setProject(projectId)
    sc.optionsAs[DataflowPipelineOptions].setRegion(region)
    sc.optionsAs[DataflowPipelineOptions].setExperiments(List("shuffle_mode=service", "flexRSGoal=OPTIMIZED").asJava)
    sc.optionsAs[DataflowPipelineOptions].setNumWorkers(1)
    sc.optionsAs[DataflowPipelineOptions].setStreaming(true)

    val sm = new SourceMerge(sc)

    sm.processSources(inputSubscription, numberInfo, resultInfoTable)

    sc.close().waitUntilFinish()
  }

  @BigQueryType.toTable
  case class NumberResults(update_time: Instant, number_sum: Int,
                           number_count: Int, number_type: String, version: String)

  @BigQueryType.fromTable("playground-bart:playground.numbers")
  class NumberInfoRow

}
