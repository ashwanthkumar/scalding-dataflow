package in.ashwanthkumar.scaldingdataflow

import java.lang.{Long => JLong}

import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory
import com.google.cloud.dataflow.sdk.transforms._
import com.google.cloud.dataflow.sdk.values.KV

object WordCount extends App {
  val withOptions = PipelineOptionsFactory
    .fromArgs(args)
    .withValidation()
    .create()
  val pipeline = Pipeline.create(withOptions)
  pipeline
    .apply(TextIO.Read.from("kinglear.txt").named("Source"))
    .apply(Count.perElement[String]())
    .apply(ParDo.named("to-string").of[KV[String, JLong], String](new DoFn[KV[String, JLong], String]() {
    override def processElement(c: DoFn[KV[String, JLong], String]#ProcessContext): Unit = {
      val kv = c.element()
      c.output(kv.getKey + "\t" + kv.getValue)
    }
  }))
    .apply(TextIO.Write.to("out.txt").named("Sink"))

  pipeline.run()
}
