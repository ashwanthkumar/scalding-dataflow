package in.ashwanthkumar.scaldingdataflow

import com.google.cloud.dataflow.sdk.options.Validation.Required
import com.google.cloud.dataflow.sdk.options.{ApplicationNameOptions, Default, Description}

trait ScaldingPipelineOptions extends ApplicationNameOptions {
  @Description("Scalding mode")
  @Default.String("local")
  def getMode: String

  def setMode(value: String)
}
