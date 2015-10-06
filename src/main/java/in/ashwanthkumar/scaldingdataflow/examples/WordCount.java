package in.ashwanthkumar.scaldingdataflow.examples;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import in.ashwanthkumar.scaldingdataflow.ScaldingPipelineOptions;

public class WordCount {
    interface Options extends ScaldingPipelineOptions {
        @Description("Input file")
        @Default.String("kinglear.txt")
        String getInput();
        void setInput(String input);

        @Description("Output file")
        @Default.String("out.txt")
        String getOutput();
        void setOutput(String output);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.Read.from(options.getInput()).named("Source"))
            .apply(ParDo.named("splitter").of(new DoFn<String, String>() {
                @Override
                public void processElement(ProcessContext c) throws Exception {
                String[] words = c.element().split("[^a-zA-Z']+");
                for (String word : words) {
                    if (!word.isEmpty()) {
                        c.output(word);
                    }
                }
            }
            }))
            .apply(Count.<String>perElement())
            .apply(ParDo.named("to-tsv").of(new DoFn<KV<String, Long>, String>() {
                @Override
                public void processElement(ProcessContext c) throws Exception {
                    KV<String, Long> kv = c.element();
                    c.output(String.format("%s\t%d", kv.getKey(), kv.getValue()));
                }
            }))
            .apply(TextIO.Write.to(options.getOutput()).named("Sink"));

        pipeline.run();
    }
}
