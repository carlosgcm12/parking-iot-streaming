package Parking.sink

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

object ElasticsearchSink {

  def escribirEnElasticsearch(df: DataFrame, checkpointPath: String): StreamingQuery = {
    df.writeStream
      .format("es")
      .option("es.nodes", "localhost")
      .option("es.port", "9200")
      .option("es.nodes.wan.only", "true")
      .option("checkpointLocation", checkpointPath)
      .outputMode("append")
      .start("parking")
  }

}
