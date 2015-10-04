package in.ashwanthkumar.scaldingdataflow

import com.google.cloud.dataflow.sdk.options.Validation.Required
import com.google.cloud.dataflow.sdk.options.{Default, Description, PipelineOptions}

trait ScaldingPipelineOptions extends PipelineOptions {
  @Description("Pipeline name")
  @Required
  def getName: String

  @Description("Scalding mode")
  @Default.String("local")
  def getMode: String

  def setName(value: String)
  def setMode(value: String)
}
