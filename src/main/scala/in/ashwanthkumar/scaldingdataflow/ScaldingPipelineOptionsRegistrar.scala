package in.ashwanthkumar.scaldingdataflow

import com.google.cloud.dataflow.sdk.options.{PipelineOptions, PipelineOptionsRegistrar}
import com.google.common.collect.ImmutableList

class ScaldingPipelineOptionsRegistrar extends PipelineOptionsRegistrar {
  override def getPipelineOptions = ImmutableList.of[Class[_ <: PipelineOptions]](classOf[ScaldingPipelineOptions])
}
