package in.ashwanthkumar.scaldingdataflow

import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory
import com.google.cloud.dataflow.sdk.transforms.{DoFn, ParDo}

object Main extends App {
  val withOptions = PipelineOptionsFactory.fromArgs(Array()).create()
  val pipeline = Pipeline.create(withOptions)
  pipeline
    .apply(TextIO.Read.from("kinglear.txt").named("Source"))
    .apply(ParDo.named("convert-to-length").of[String, Int](new DoFn[String, Int]() {
    override def processElement(c: DoFn[String, Int]#ProcessContext): Unit = c.output(c.element().length)
  }))
    .apply(ParDo.named("to-string").of[Int, String](new DoFn[Int, String]() {
    override def processElement(c: DoFn[Int, String]#ProcessContext): Unit = c.output(c.element().toString)
  }))
    .apply(TextIO.Write.to("out.txt").named("Sink"))

  ScaldingRunner.local("init").run(pipeline)
}
