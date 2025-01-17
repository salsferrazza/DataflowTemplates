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
 * A template that extracts the OHLC bars from a pricing time series.
 */
public class CandlestickPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(CandlestickPublisher.class);

    /**
     * Main entry point for executing the pipeline.
     *
     * @param args The command-line arguments to the pipeline.
     */
    public static void main(String[] args) {

        // Parse the user options passed from the command-line
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        options.setStreaming(true);
        options.setAppName("CandlestickPublisher");

        List<String> keysOfInterest = new ArrayList<String>();
	keysOfInterest.add(options.getSymbol().get());
        LOG.info(String.format("Keys used for VWAP are: %s", keysOfInterest));
        options.setVWAPMajorKeyName(keysOfInterest);
        options.setVWAPPriceName(options.getPriceFieldName().get());

        // Type 1 time length and Type 2 time length
        int interval = options.getInterval().get();
        options.setTypeOneComputationsLengthInSecs(interval);
        options.setTypeTwoComputationsLengthInSecs(interval);

        // How many timesteps to output
        options.setOutputTimestepLengthInSecs(interval);

        // Parameters for gap filling.
        // We want to ensure that there is always a value within each timestep. This is redundant for
        // this dataset as the generated data will always have a value. But we keep this configuration
        // to ensure consistency across the sample pipelines.
        options.setGapFillEnabled(true);
        options.setEnableHoldAndPropogateLastValue(false);
        options.setTTLDurationSecs(1);

        // Setup the metrics that are to be computed
        options.setTypeOneBasicMetrics(ImmutableList.of(
            "typeone.Min", 
            "typeone.Max"));
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
            //Add filter for symbol
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
            //Extract OHLC
            //Map to output template
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

	@Description("The symbol of the instrument from which to derive the candlesticks")		
        @Validation.Required
	ValueProvider<String> getSymbol();
	void setSymbol(ValueProvider<String> symbol);

        @Description("The output message template for the VWAP emission. Can either be set inline or if the string begins with \"gs://\", the value with be retreived from Cloud Storage upon pipeline launch.")
	ValueProvider<String> getOutputMessageTemplate();
	void setOutputMessageTemplate(ValueProvider<String> outputMessageTemplate);
	
	
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

        @Description("Enable printing metrics to logs")
        Boolean getEnablePrintMetricsToLogs();
        void setEnablePrintMetricsToLogs(Boolean enablePrintMetricsToLogs);
	
        @Description("Enable printing output to logs")
        Boolean getEnablePrintTFExamplesToLogs();
        void setEnablePrintTFExamplesToLogs(Boolean enablePrintTFExamplesToLogs);

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
            return new AutoValue_CandlestickPublisher_CreateOutputMessageFn.Builder();
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
    public abstract static class DeriveCandlestickFn extends DoFn<PubsubMessage, PubsubMessage> {

        private static final Logger LOG = LoggerFactory.getLogger(DeriveCandlestickFn.class);

        // Counter tracking the number of incoming Pub/Sub messages.
        private static final Counter INPUT_COUNTER = Metrics.counter(DeriveCandlestickFn.class, "inbound-messages");

        // Counter tracking the number of output Pub/Sub messages after the user
        // provided filter
        // is applied.
        private static final Counter OUTPUT_COUNTER = Metrics.counter(DeriveCandlestickFn.class, "outbound-messages");

        // Pipeline options
        public abstract Options getOptions();

        public static Builder newBuilder() {
            return new AutoValue_CandlestickPublisher_DeriveCandlestickFn.Builder();
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

        /** Builder class for {@link DeriveCandlestickFn}. */
        @AutoValue.Builder
        abstract static class Builder {
            abstract DeriveCandlestickFn build();

            abstract Builder setOptions(Options options);
        }
    }
}
