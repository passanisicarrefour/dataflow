// package com.google.cloud.teleport.templates;

package com.google.cloud.training.dataanalyst.javahelp;

//import static com.google.cloud.teleport.templates.TextToBigQueryStreaming.wrapBigQueryInsertError;

import com.google.api.services.bigquery.model.TableRow;
//import com.google.cloud.teleport.coders.FailsafeElementCoder;
//import com.google.cloud.teleport.templates.common.BigQueryConverters.FailsafeJsonToTableRow;
//import com.google.cloud.teleport.templates.common.ErrorConverters;
//import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.FailsafeJavascriptUdf;
//import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
//import com.google.cloud.teleport.util.DualInputNestedValueProvider;
//import com.google.cloud.teleport.util.DualInputNestedValueProvider.TranslatorInput;
//import com.google.cloud.teleport.util.ResourceUtils;
//import com.google.cloud.teleport.util.ValueProviderUtils;
//import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.collect.ImmutableList;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiPubSubToBigQuery {


/** The log to output status messages to. *//*

    private static final Logger LOG = LoggerFactory.getLogger(PubSubToBigQuery.class);

    */
/** The tag for the main output for the UDF. *//*

    public static final TupleTag<FailsafeElement<PubsubMessage, String>> UDF_OUT =
            new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

    */
/** The tag for the main output of the json transformation. *//*

    public static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<TableRow>() {};

    */
/** The tag for the dead-letter output of the udf. *//*

    public static final TupleTag<FailsafeElement<PubsubMessage, String>> UDF_DEADLETTER_OUT =
            new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

    */
/** The tag for the dead-letter output of the json to table row transform. *//*

    public static final TupleTag<FailsafeElement<PubsubMessage, String>> TRANSFORM_DEADLETTER_OUT =
            new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

    */
/** The default suffix for error tables if dead letter table is not specified. *//*

    public static final String DEFAULT_DEADLETTER_TABLE_SUFFIX = "_error_records";

    */
/** Pubsub message/string coder for pipeline. *//*

    public static final FailsafeElementCoder<PubsubMessage, String> CODER =
            FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

    */
/** String/String Coder for FailsafeElement. *//*

    public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
            FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    */

    /**
     * The {@link Options} class provides the custom execution options passed by the executor at the
     * command-line.
     */


    public interface Options extends PipelineOptions, JavascriptTextTransformerOptions {
        @Description("Table spec to write the output to")
        ValueProvider<String> getOutputTableSpec();

        void setOutputTableSpec(ValueProvider<String> value);

        @Description("Pub/Sub topic to read the input from")
        ValueProvider<String> getInputTopic();

        void setInputTopic(ValueProvider<String> value);

        @Description(
                "The Cloud Pub/Sub subscription to consume from. "
                        + "The name should be in the format of "
                        + "projects/<project-id>/subscriptions/<subscription-name>.")
        ValueProvider<String> getInputSubscription();

        void setInputSubscription(ValueProvider<String> value);

        @Description(
                "This determines whether the template reads from " + "a pub/sub subscription or a topic")
        @Default.Boolean(false)
        Boolean getUseSubscription();

        void setUseSubscription(Boolean value);

        @Description(
                "The dead-letter table to output to within BigQuery in <project-id>:<dataset>.<table> "
                        + "format. If it doesn't exist, it will be created during pipeline execution.")
        ValueProvider<String> getOutputDeadletterTable();

        void setOutputDeadletterTable(ValueProvider<String> value);
    }

    /*
    private static final String topics[] = {
            "vm_store",
            "vm_promo",
            "vm_prod",
            "vm_fam_grp",
            "tr_segment",
            "tr_promo",
            "tr_prodband",
            "tr_parp_ems",
            "tr_household",
            "tr_giftcard",
            "tr_consumer",
            "tr_chaintype",
            "tr_cdmsegment",
            "tr_allpromo",
            "tp_refpromo",
            "tp_nielsen",
            "tf_ticket",
            "tf_tender",
            "tf_promotic",
            "tf_promoref",
            "tf_daysales",
            "tf_contact",
            "tf_bx_emis",
            "tf_allpromo"
    };

     */

    private static final String topics[] = {
            "testTopic",
            "testTopic2",
    };

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        // projects/pj-becfr-bigdata-dev-282911/topics/dev_dmm_vm_store

        final String PROJECT = "pj-becfr-bigdata-dev-282911";
        final String PREFIX_TOPIC = "projects/" + PROJECT + "/topics/dev_dmm_";

        Pipeline pipeline = Pipeline.create(options);

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(CODER.getEncodedTypeDescriptor(), CODER);

        // Step #1: Read messages in from Pub/Sub

        for (String t : topics) {
            PCollection<PubsubMessage> messages = pipeline
                    .apply("ReadPubSubTopic",
                            // PubsubIO.readMessagesWithAttributes().fromTopic(PREFIX_TOPIC + t));
                            PubsubIO.readMessagesWithAttributes().fromTopic(t));

            PCollectionTuple convertedTableRows = messages

                    // Step #2: Transform the PubsubMessages into TableRows

                    .apply("ConvertMessageToTableRow", new PubsubMessageToTableRow(options));

            // Step #3: Write the successful records out to BigQuery

            WriteResult writeResult = convertedTableRows
                    .get(TRANSFORM_OUT)
                    .apply("WriteSuccessfulRecords",
                            BigQueryIO.writeTableRows()
                                    .withoutValidation()
                                    .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                                    .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                                    .withExtendedErrorInfo()
                                    .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                                    .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                                    //.to(PROJECT + ":raw_dmm." + t));
                                    .to(PROJECT + ":test_pipeline." + t));

            pipeline.run();

        }
    }

    /**
     * If deadletterTable is available, it is returned as is, otherwise outputTableSpec +
     * defaultDeadLetterTableSuffix is returned instead.
     */


    /*
    private static ValueProvider<String> maybeUseDefaultDeadletterTable(
            ValueProvider<String> deadletterTable,
            ValueProvider<String> outputTableSpec,
            String defaultDeadLetterTableSuffix) {
        return DualInputNestedValueProvider.of(
                deadletterTable,
                outputTableSpec,
                new SerializableFunction<TranslatorInput<String, String>, String>() {
                    @Override
                    public String apply(TranslatorInput<String, String> input) {
                        String userProvidedTable = input.getX();
                        String outputTableSpec = input.getY();
                        if (userProvidedTable == null) {
                            return outputTableSpec + defaultDeadLetterTableSuffix;
                        }
                        return userProvidedTable;
                    }
                });
    }
*/

    static class PubsubMessageToTableRow
            extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

        private final Options options;

        PubsubMessageToTableRow(Options options) {
            this.options = options;
        }

        @Override
        public PCollectionTuple expand(PCollection<PubsubMessage> input) {

            PCollectionTuple udfOut =
                    input
                            // Map the incoming messages into FailsafeElements so we can recover from failures
                            // across multiple transforms.
                            .apply("MapToRecord", ParDo.of(new PubsubMessageToFailsafeElementFn()))
                            /*.apply(
                                    "InvokeUDF",
                                    FailsafeJavascriptUdf.<PubsubMessage>newBuilder()
                                            .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                                            .setFunctionName(options.getJavascriptTextTransformFunctionName())
                                            .setSuccessTag(UDF_OUT)
                                            .setFailureTag(UDF_DEADLETTER_OUT)
                                            .build());
                                            
                             */

            // Convert the records which were successfully processed by the UDF into TableRow objects.
            PCollectionTuple jsonToTableRowOut =
                    udfOut
                            .get(UDF_OUT)
                            .apply(
                                    "JsonToTableRow",
                                    FailsafeJsonToTableRow.<PubsubMessage>newBuilder()
                                            .setSuccessTag(TRANSFORM_OUT)
                                            .setFailureTag(TRANSFORM_DEADLETTER_OUT)
                                            .build());

            // Re-wrap the PCollections so we can return a single PCollectionTuple
            return PCollectionTuple.of(UDF_OUT, udfOut.get(UDF_OUT))
                    .and(UDF_DEADLETTER_OUT, udfOut.get(UDF_DEADLETTER_OUT))
                    .and(TRANSFORM_OUT, jsonToTableRowOut.get(TRANSFORM_OUT))
                    .and(TRANSFORM_DEADLETTER_OUT, jsonToTableRowOut.get(TRANSFORM_DEADLETTER_OUT));
        }
    }


    /**
     * The {@link PubsubMessageToFailsafeElementFn} wraps an incoming {@link PubsubMessage} with the
     * {@link FailsafeElement} class so errors can be recovered from and the original message can be
     * output to a error records table.
     */
/*
    static class PubsubMessageToFailsafeElementFn
            extends DoFn<PubsubMessage, FailsafeElement<PubsubMessage, String>> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            PubsubMessage message = context.element();
            context.output(
                    FailsafeElement.of(message, new String(message.getPayload(), StandardCharsets.UTF_8)));
        }
    }


 */
}