package in.ashwanthkumar.scaldingdataflow

import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory

object Main extends App {
  val withOptions = PipelineOptionsFactory.fromArgs(Array()).create()
  val pipeline = Pipeline.create(withOptions)
  pipeline
    .apply(TextIO.Read.from("kinglear.txt").named("Source"))
    .apply(TextIO.Write.to("out.txt").named("Sink"))

  ScaldingRunner.local("init").run(pipeline)
}
