package com.iomete.spark.kafkastreaming

import org.apache.spark.sql.DataFrame

abstract class StreamProcessor {

  /**
   * Responsible for processing spark streaming.
   */
  def process(): Unit

  /**
   * Responsible for processing micro batches for every batch processing.
   *
   * @param batchDF Batch dataframe to be wrote.
   * @param batchId Micro batch ID.
   */
  def microBatch(batchDF: DataFrame, batchId: Long): Unit

}
