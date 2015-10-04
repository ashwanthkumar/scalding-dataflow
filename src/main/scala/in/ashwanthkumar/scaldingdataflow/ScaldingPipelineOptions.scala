package in.ashwanthkumar.scaldingdataflow

import com.google.cloud.dataflow.sdk.options.{Default, Description, PipelineOptions}

trait ScaldingPipelineOptions extends PipelineOptions {
  @Description("The url of hadoop master")
  @Default.String("local")
  def getMasterUrl: String
  def setMasterUrl(value: String)
}
