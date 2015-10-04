package in.ashwanthkumar.scaldingdataflow

import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory
import com.google.cloud.dataflow.sdk.transforms.{DoFn, Filter, ParDo}

object Main extends App {
  val withOptions = PipelineOptionsFactory.fromArgs(Array()).create()
  val pipeline = Pipeline.create(withOptions)
  pipeline
    .apply(TextIO.Read.from("kinglear.txt").named("Source"))
    .apply(ParDo.named("convert-to-length").of[String, Integer](new DoFn[String, Integer]() {
    override def processElement(c: DoFn[String, Integer]#ProcessContext): Unit = c.output(c.element().length)
  }))
    .apply(Filter.greaterThan[Integer](10))
    .apply(ParDo.named("to-string").of[Integer, String](new DoFn[Integer, String]() {
    override def processElement(c: DoFn[Integer, String]#ProcessContext): Unit = c.output(c.element().toString)
  }))
    .apply(TextIO.Write.to("out.txt").named("Sink"))

  ScaldingPipelineRunner.local("init").run(pipeline)
}
