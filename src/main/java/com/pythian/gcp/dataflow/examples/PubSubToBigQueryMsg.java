package com.pythian.gcp.dataflow.examples;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

import org.joda.time.Instant;
import org.joda.time.Duration;
import java.sql.Timestamp;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.apache.commons.codec.binary.Base64;
import java.nio.charset.StandardCharsets;



public class PubSubToBigQueryMsg {
    
  public static void main(String[] args) {
    DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);
    // Override the default worker size to the smallest possible
    options.setDiskSizeGb(4);
    options.setWorkerMachineType("n1-standard-1");
    // Set job mode to streaming
    options.setStreaming(true);
    options.setRunner(DataflowRunner.class);

    // Topic to pull data from
    String TOPIC_NAME = "projects/" + options.getProject() + "/topics/streamdemo";
    // BigQuery table location to write to
    // need to create the demos dataset first
    String BQ_DS = options.getProject() + ":demos.psdemo";
    
    // Build the table schema for the output table.
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("message").setType("STRING"));
    TableSchema schema = new TableSchema().setFields(fields);
   
    // Start the pipeline with reading messages from PubSub
    Pipeline p = Pipeline.create(options);
    PCollection<TableRow> orders = p 
    .apply(PubsubIO.readStrings().fromTopic(TOPIC_NAME))
    .apply("ConvertDataToTableRows", ParDo.of(new DoFn<String, TableRow>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        String message = c.element();
        TableRow row = new TableRow()
              .set("message", message);
        c.output(row);
      }
    }));

    // write the pCollection to BQ
    orders.apply("InsertTableRowsToBigQuery",
      BigQueryIO.writeTableRows().to(BQ_DS)
      .withSchema(schema)
      .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
      .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

    // Run the pipeline
    p.run();
  }
}
