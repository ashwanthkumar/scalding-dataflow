package in.ashwanthkumar.scaldingdataflow.examples;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;

public class WordCount {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.Read.from("kinglear.txt").named("Source"))
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
            .apply(TextIO.Write.to("out.txt").named("Sink"));

        pipeline.run();
    }
}
