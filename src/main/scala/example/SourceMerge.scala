package example

import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.{SCollection, SideInput, WindowOptions}
import com.twitter.algebird.{Aggregator, Moments, MultiAggregator}
import com.typesafe.config.{Config, ConfigFactory}
import example.proto.message.NumberBuffer
import org.apache.beam.runners.dataflow.DataflowRunner
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.transforms.windowing.{AfterWatermark, IntervalWindow, Repeatedly, TimestampCombiner}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.{DateTime, Duration, Instant}

import scala.collection.JavaConverters._
import scala.language.higherKinds
import scala.util.Try

case class NumberAggregate(sum: Double, count: Long, numberType: Option[String] = None) {
  /**
   * Find string that describes the order of magnitude of a number.
   *
   * @param numberInfoMap : Map with lower bounds, upper bounds and resulting string.
   * @return Optional output string.
   */
  def findNumberString(numberInfoMap: Seq[(Long, Long, String)]): Option[String] =
    Try(
      numberInfoMap
        .filter { case (lower: Long, upper: Long, _) => (lower <= this.sum) && (upper >= this.sum) }
        .map(x => x._3)
        .head)
      .toOption
}

/**
 * Process Pub/Sub source and BigQuery table.
 *
 * @param sc                : ScioContext.
 * @param inputSubscription : Path to subscription.
 * @param numberInfoTable   : BigQuery table reference.
 * @param resultInfoTable   : Table to write results to.
 */
class SourceMerge(@transient val sc: ScioContext, inputSubscription: String,
                  numberInfoTable: String, resultInfoTable: String,
                  windowSeconds: Int, allowedLateness: Int) extends Serializable {

  import example.SourceMerge.NumberResults

  /**
   * Process data.
   */
  def processSources(): Unit =
    getInputStream
      .windowedStream
      .aggregateValues(numberInfoTable)
      .transformToTableRow()
      .saveAsCustomOutput("Save aggregate to BQ",
        BigQueryIO
          .writeTableRows()
          .to(resultInfoTable)
          .withSchema(BigQueryType[NumberResults].schema)
          .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
          .withWriteDisposition(WriteDisposition.WRITE_APPEND))

  /**
   * Load parsed stream of messages from Google Cloud Pub/Sub.
   *
   * @return SCollection of parsed messages.
   */
  def getInputStream: SCollection[NumberBuffer] = sc
    .pubsubSubscription[Array[Byte]](inputSubscription)
    .transform("Parse objects")(
      _.map(NumberBuffer.parseFrom))

  implicit class scSideInputProcessor(pipeline: SCollection[NumberBuffer]) {
    /**
     * Windowed stream with sum and window information.
     *
     * @return Windowed SCollection with time window and sum.
     */
    def windowedStream: SCollection[NumberBuffer] =
      pipeline
        .transform("Timestamp records")(
          _.timestampBy(x =>
            new DateTime(x.timestamp).toInstant, allowedTimestampSkew = Duration.standardSeconds(allowedLateness)))
        .transform("Apply window functions")(
          _.withFixedWindows(Duration.standardSeconds(windowSeconds),
            options = WindowOptions(
              timestampCombiner = TimestampCombiner.END_OF_WINDOW,
              accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
              trigger = Repeatedly.forever(AfterWatermark.pastEndOfWindow()),
              allowedLateness = Duration.standardSeconds(allowedLateness))))

    /**
     * Add side input and aggregate values.
     *
     * @param numberInfoTable : Number info table to use.
     * @return SCollection with time window and summed value.
     */
    def aggregateValues(numberInfoTable: String): SCollection[NumberAggregate] = {
      val numberInfoSideInput = getNumberInfoSideInput(numberInfoTable)

      val sumOp = Aggregator.numericSum[Int].composePrepare[NumberBuffer](_.number)
      val momentsOp = Moments.aggregator.composePrepare[NumberBuffer](_.number)

      val colAggregate = MultiAggregator((sumOp, momentsOp)).andThenPresent {
        case (sum, moments) => NumberAggregate(sum, moments.count)
      }

      pipeline
        .aggregate(colAggregate)
        .transform("Add side input")(
          // Since the right side is small, a side input can be used.
          _.withSideInputs(numberInfoSideInput)
            .map {
              case (aggregate, side) =>
                aggregate.copy(
                  sum = aggregate.sum,
                  count = aggregate.count,
                  numberType = aggregate.findNumberString(side(numberInfoSideInput)))
            }
            .toSCollection
        )
    }
  }

  implicit class scJoinProcessor(pipeline: SCollection[NumberAggregate]) {
    /**
     * Transform joined data into TableRow.
     *
     * @return SCollection of TableRows
     */
    def transformToTableRow(): SCollection[TableRow] = {
      pipeline
        .transform("Add window information")(
          _.withWindow[IntervalWindow]
            .map { case (aggregate, window) => (window.end(), aggregate) })
        .transform("Convert into TableRow")(
          _.map {
            case (timestamp, aggregate) => TableRow(Map(
              "update_time" -> Timestamp(timestamp),
              "number_sum" -> aggregate.sum,
              "number_count" -> aggregate.count,
              "number_type" -> aggregate.numberType.getOrElse("unknown"),
              "version" -> "v1")
              .map(kv => (kv._1, kv._2))
              .toList: _*)
          })
    }
  }

  /**
   * Load typed BigQuery table which contains information about the numbers.
   *
   * @param tableSpec : Table name to load.
   * @return Side input
   */
  def getNumberInfoSideInput(tableSpec: String): SideInput[Seq[(Long, Long, String)]] = sc
    .customInput(s"Read number info",
      BigQueryIO
        .readTableRows()
        .from(tableSpec))
    .transform("Extract boundaries")(
      _.map(x => (
        x.getLong("lower_bound"),
        x.getLong("upper_bound"),
        x.getString("number_type")))
    ).asListSideInput
}

object SourceMerge {
  val config: Config = ConfigFactory.load()
  val projectId: String = config.getString("pipeline.project_id")
  val bucketPath: String = config.getString("pipeline.bucket")
  val inputSubscription: String = config.getString("pipeline.subscription_input")
  val region: String = config.getString("pipeline.region")
  val numberInfo: String = config.getString("pipeline.number_info_table")
  val resultInfoTable: String = config.getString("pipeline.result_table")
  val windowSeconds: Int = config.getInt("pipeline.window_seconds")
  val allowedLateness: Int = config.getInt("pipeline.allowed_lateness_seconds")

  /**
   * Main method
   *
   * @param cmdlineArgs : Command-line arguments
   */
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, _) = ContextAndArgs(cmdlineArgs)

    sc.options.setJobName("example-etl-" + DateTime.now().toString("YYYY-MM-dd-HHmmss"))
    sc.optionsAs[DataflowPipelineOptions].setProject(projectId)
    sc.optionsAs[DataflowPipelineOptions].setRegion(region)
    sc.optionsAs[DataflowPipelineOptions].setWorkerMachineType("n1-standard-2")
    sc.optionsAs[DataflowPipelineOptions].setExperiments(List("flexRSGoal=OPTIMIZED").asJava)
    sc.optionsAs[DataflowPipelineOptions].setNumWorkers(1)
    sc.optionsAs[DataflowPipelineOptions].setTempLocation(bucketPath + "/temp")
    sc.optionsAs[DataflowPipelineOptions].setStagingLocation(bucketPath + "/staging")
    sc.optionsAs[DataflowPipelineOptions].setStreaming(true)

    sc.options.setRunner(classOf[DataflowRunner])

    val sm = new SourceMerge(
      sc,
      inputSubscription,
      numberInfo,
      resultInfoTable,
      windowSeconds,
      allowedLateness)

    sm.processSources()

    sc.run().waitUntilFinish()
  }

  @BigQueryType.toTable
  case class NumberResults(update_time: Instant,
                           number_sum: Int,
                           number_count: Int,
                           number_type: String,
                           version: String)

}
