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

package com.google.cloud.teleport.templates; 

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An template that copies messages from one Pubsub subscription to another Pubsub topic. */
public class VwapPublisher {

    /**
     * Main entry point for executing the pipeline.
     *
     * @param args The command-line arguments to the pipeline.
     */
    public static void main(String[] args) {

        // Parse the user options passed from the command-line
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        options.setStreaming(true);

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
         * Steps:
         *      1) Read PubSubMessage with attributes from input PubSub subscription.
         *      2) Write each PubSubMessage to output PubSub topic.
         */
        pipeline
            .apply(
                   "Read PubSub Events",
                   PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic()))
            .apply(
                   "Derive VWAP",
                   ParDo.of(
                            DeriveVwapFn.newBuilder()
                            .build()))
            .apply("Write PubSub Events", PubsubIO.writeMessages().to(options.getOutputTopic()));

        // Execute the pipeline and return the result.
        return pipeline.run();
    }

    /**
     * Options supported by {@link PubsubToPubsub}.
     *
     * <p>Inherits standard configuration options.
     */
    public interface Options extends PipelineOptions, StreamingOptions {
        @Description(
                     "The Cloud Pub/Sub topic to consume from. "
                     + "The name should be in the format of "
                     + "projects/<project-id>/topics/<topic-name>.")
                     @Validation.Required
            ValueProvider<String> getInputTopic();

        void setInputTopic(ValueProvider<String> inputTopic);
      
        @Description(
                     "The time interval, in seconds, for the VWAP lookback window. "
                     + "For a 5 minute VWAP window, specify 300")
                     @Validation.Required
            ValueProvider<Integer> getInterval();
      
        void setInterval(ValueProvider<Integer> interval);

        @Description(
                     "The field name to use in the JSON payload for the price")
                     @Validation.Required
            ValueProvider<String> getPriceFieldName();
      
        void setPriceFieldName(ValueProvider<String> priceFieldName);
            
      
        @Description(
                     "The field name to use in the JSON payload for the volume")
                     @Validation.Required
            ValueProvider<String> getVolumeFieldName();
      
        void setVolumeFieldName(ValueProvider<String> volumeFieldName);

        
        @Description(
                     "The Cloud Pub/Sub topic to publish to. "
                     + "The name should be in the format of "
                     + "projects/<project-id>/topics/<topic-name>.")
                     @Validation.Required
            ValueProvider<String> getOutputTopic();

        void setOutputTopic(ValueProvider<String> outputTopic);




    }

    /**
     * DoFn that will determine if events are to be filtered. If filtering is enabled, it will only
     * publish events that pass the filter else, it will publish all input events.
     */
    @AutoValue
    public abstract static class DeriveVwapFn extends DoFn<PubsubMessage, PubsubMessage> {

        private static final Logger LOG = LoggerFactory.getLogger(DeriveVwapFn.class);

        // Counter tracking the number of incoming Pub/Sub messages.
        private static final Counter INPUT_COUNTER = Metrics
            .counter(DeriveVwapFn.class, "inbound-messages");

        // Counter tracking the number of output Pub/Sub messages after the user provided filter
        // is applied.
        private static final Counter OUTPUT_COUNTER = Metrics
            .counter(DeriveVwapFn.class, "outbound-messages");

        public static Builder newBuilder() {
            return new AutoValue_VwapPublisher_DeriveVwapFn.Builder();
        }

        @Setup
        public void setup() {   }

        @ProcessElement
        public void processElement(ProcessContext context) {

            INPUT_COUNTER.inc();
            PubsubMessage message = context.element();
            writeOutput(context, message);
        
        }

        /**
         * Write a {@link PubsubMessage} and increment the output counter.
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
	}
    }
}
