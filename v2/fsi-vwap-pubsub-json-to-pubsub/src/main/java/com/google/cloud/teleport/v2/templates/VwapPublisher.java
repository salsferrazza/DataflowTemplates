/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.v2.templates;

import com.google.auto.value.AutoValue;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData;
import com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.CMEAdapter.SSCLTRDJsonTransform;
import com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.DeadLetterSink;
import com.google.dataflow.sample.timeseriesflow.graph.GenerateComputations;
import com.google.dataflow.sample.timeseriesflow.metrics.core.TSMetricsOptions;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.ma.MAFn.MAAvgComputeMethod;
import com.google.dataflow.sample.timeseriesflow.options.TFXOptions;
import com.google.dataflow.sample.timeseriesflow.options.TSOutputPipelineOptions;
import com.google.dataflow.sample.timeseriesflow.transforms.TSAccumToJson;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A template that calculates the Volume Weighted Average Price of a time-series
 * stream who's messages contain embedded price and volume properties.
 */
public class VwapPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(VwapPublisher.class);

    /**
     * Main entry point for executing the pipeline.
     *
     * @param args The command-line arguments to the pipeline.
     */
    public static void main(String[] args) {

        // Parse the user options passed from the command-line
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        options.setStreaming(true);
        options.setAppName("VwapPublisher");

        Map<String, Integer> monthMap = new HashMap<>();
        monthMap.put("F", 1);
        monthMap.put("G", 2);
        monthMap.put("H", 3);
        monthMap.put("J", 4);
        monthMap.put("K", 5);
        monthMap.put("M", 6);
        monthMap.put("N", 7);
        monthMap.put("Q", 8);
        monthMap.put("U", 9);
        monthMap.put("V", 10);
        monthMap.put("X", 11);
        monthMap.put("Z", 12);

        List<String> keysOfInterest = new ArrayList<>();
        ImmutableList.of("CL")
            .forEach(x -> monthMap.keySet().forEach(y -> keysOfInterest.add(x + y + "1")));
        LOG.info(String.format("Keys used for VWAP are: %s", keysOfInterest));
        options.setVWAPMajorKeyName(keysOfInterest);
        options.setVWAPPriceName(options.getPriceFieldName().get());

        // Type 1 time length and Type 2 time length
        int interval = options.getInterval().get();
        options.setTypeOneComputationsLengthInSecs(5);
        options.setTypeTwoComputationsLengthInSecs(interval);

        // How many timesteps to output
        options.setOutputTimestepLengthInSecs(5);

        // Parameters for gap filling.
        // We want to ensure that there is always a value within each timestep. This is redundant for
        // this dataset as the generated data will always have a value. But we keep this configuration
        // to ensure consistency across the sample pipelines.
        options.setGapFillEnabled(true);
        options.setEnableHoldAndPropogateLastValue(false);
        options.setTTLDurationSecs(1);

        // Setup the metrics that are to be computed
        options.setTypeOneBasicMetrics(ImmutableList.of(
            "typeone.Sum", 
            "typeone.Min", 
            "typeone.Max"));
        options.setTypeTwoBasicMetrics(ImmutableList.of(
            "typetwo.basic.ma.MAFn", 
            "typetwo.basic.stddev.StdDevFn"));
        options.setTypeTwoComplexMetrics(ImmutableList.of(
            "complex.fsi.vwap.VWAPGFn"));
        options.setMAAvgComputeMethod(MAAvgComputeMethod.SIMPLE_MOVING_AVERAGE);

        run(options);
    }

    /**
     * Runs the pipeline with the supplied options.
     *
     * @param options The execution parameters to the pipeline.
     * @return The result of the pipeline execution.
     */
    public static PipelineResult run(Options options) {
        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        /**
         * Steps: 1) Read PubSubMessage strings from input PubSub topic 2) Use
         * CMEAdapter.SSCLTRDJsonTransform to convert these to TSDataPoints 3) TODO:
         * Perform Type 1 or 2 calcs on the resulting PCollection<TSDataPoints>, and
         * publish somewhere. This'll probably happen in the DeriveVwapFn I imagine.
         */

        PCollection<TimeSeriesData.TSDataPoint> sources = pipeline
            .apply("Read PubSub Events", PubsubIO.readStrings().fromTopic(options.getInputTopic())
                .withTimestampAttribute("EarliestEventTime"))

            .apply("Convert Pub/Sub messages to time-series data points",
                SSCLTRDJsonTransform.newBuilder()
                    .setDeadLetterSinkType(DeadLetterSink.LOG)
                    .setBigQueryDeadLetterSinkProject(null)
                    .setBigQueryDeadLetterSinkTable(null)
                    .setSuppressCategorical(true)
                    .build());

        GenerateComputations generateComputations =
                GenerateComputations.fromPiplineOptions(options).build();
                        
        sources
            .apply("Generate computations", generateComputations)
            .apply("Convert output to Json", TSAccumToJson.create())
            .apply("Convert Json to Pub/Sub message", ParDo.of(CreateOutputMessageFn.newBuilder().build()))
            .apply("Write Pub/Sub events", PubsubIO.writeMessages().to(options.getOutputTopic()));
         
        // Execute the pipeline and return the result.
        return pipeline.run();

    }

    /**
     * Options supported by {@link PubsubToPubsub}.
     *
     * <p>
     * Inherits standard configuration options.
     */
    public interface Options
            extends TSOutputPipelineOptions, TSMetricsOptions, TFXOptions {
        @Description("The Cloud Pub/Sub topic to consume from. " + "The name should be in the format of "
                + "projects/<project-id>/topics/<topic-name>.")
        @Validation.Required
        ValueProvider<String> getInputTopic();

        void setInputTopic(ValueProvider<String> inputTopic);

        @Description("The time interval, in seconds, for the VWAP lookback window. "
                + "For a 5 minute VWAP window, specify 300")
        @Validation.Required
        ValueProvider<Integer> getInterval();

        void setInterval(ValueProvider<Integer> interval);

        @Description("The field name to use in the JSON payload for the price")
        @Validation.Required
        ValueProvider<String> getPriceFieldName();

        void setPriceFieldName(ValueProvider<String> priceFieldName);

        @Description("The field name to use in the JSON payload for the volume")
        @Validation.Required
        ValueProvider<String> getVolumeFieldName();

        void setVolumeFieldName(ValueProvider<String> volumeFieldName);

        @Description("The Cloud Pub/Sub topic to publish to. " + "The name should be in the format of "
        + "projects/<project-id>/topics/<topic-name>.")
        @Validation.Required
        ValueProvider<String> getOutputTopic();

        void setOutputTopic(ValueProvider<String> outputTopic); 
    }

    /**
     * DoFn that will convert TSDataPoints to PubsubMessages.
     */
    @AutoValue
    public abstract static class CreateOutputMessageFn extends DoFn<String, PubsubMessage> {

        private static final Logger LOG = LoggerFactory.getLogger(CreateOutputMessageFn.class);

        // Counter tracking the number of incoming Pub/Sub messages.
        private static final Counter INPUT_COUNTER = Metrics.counter(CreateOutputMessageFn.class, "inbound-messages");

        // Counter tracking the number of output Pub/Sub messages after the user
        // provided filter
        // is applied.
        private static final Counter OUTPUT_COUNTER = Metrics.counter(CreateOutputMessageFn.class, "outbound-messages");

        public static Builder newBuilder() {
            return new AutoValue_VwapPublisher_CreateOutputMessageFn.Builder();
        }

        @Setup
        public void setup() {
        }

        @ProcessElement
        public void processElement(ProcessContext context) {
            INPUT_COUNTER.inc();
            String json = context.element();
            PubsubMessage msg = new PubsubMessage(json.getBytes(), new HashMap<String, String>());
            context.output(msg);
            OUTPUT_COUNTER.inc();
        }

        /** Builder class for {@link CreateOutputMessageFn}. */
        @AutoValue.Builder
        abstract static class Builder {
            abstract CreateOutputMessageFn build();
        }
    }

    /**
     * DoFn that will determine if events are to be filtered. If filtering is
     * enabled, it will only publish events that pass the filter else, it will
     * publish all input events.
     */
    @AutoValue
    public abstract static class DeriveVwapFn extends DoFn<PubsubMessage, PubsubMessage> {

        private static final Logger LOG = LoggerFactory.getLogger(DeriveVwapFn.class);

        // Counter tracking the number of incoming Pub/Sub messages.
        private static final Counter INPUT_COUNTER = Metrics.counter(DeriveVwapFn.class, "inbound-messages");

        // Counter tracking the number of output Pub/Sub messages after the user
        // provided filter
        // is applied.
        private static final Counter OUTPUT_COUNTER = Metrics.counter(DeriveVwapFn.class, "outbound-messages");

        // Pipeline options
        public abstract Options getOptions();

        public static Builder newBuilder() {
            return new AutoValue_VwapPublisher_DeriveVwapFn.Builder();
        }

        @Setup
        public void setup() {
        }

        @ProcessElement
        public void processElement(ProcessContext context) {

            INPUT_COUNTER.inc();
            PubsubMessage message = context.element();
            writeOutput(context, message);

        }

        /**
         * Write a {@link PubsubMessage} and increment the output counter.
         * 
         * @param context {@link ProcessContext} to write {@link PubsubMessage} to.
         * @param message {@link PubsubMessage} output.
         */
        private void writeOutput(ProcessContext context, PubsubMessage message) {
            OUTPUT_COUNTER.inc();
            context.output(message);
        }

        /** Builder class for {@link DeriveVWapFn}. */
        @AutoValue.Builder
        abstract static class Builder {
            abstract DeriveVwapFn build();

            abstract Builder setOptions(Options options);
        }
    }
}
