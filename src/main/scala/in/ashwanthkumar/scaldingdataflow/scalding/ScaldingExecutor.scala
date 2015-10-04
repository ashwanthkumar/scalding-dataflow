package in.ashwanthkumar.scaldingdataflow.scalding

import com.twitter.scalding.Config
import com.twitter.scalding.typed.TypedPipe
import in.ashwanthkumar.scaldingdataflow.{SContext, SPipe}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

// Force execution of the pipe to get results for sideInputs
// FIXME - Find a better way to do this
object ScaldingExecutor {
  private val LOG = LoggerFactory.getLogger(ScaldingExecutor.getClass)

  def asIterable[T](pipe: SPipe, ctx: SContext): Iterable[T] = {
    LOG.info("Executing the pipe")
    val exec = pipe.pipe.asInstanceOf[TypedPipe[T]]
      .toIterableExecution
      .waitFor(Config.default, ctx.mode)
    LOG.info("Execution of the pipe complete")

    exec match {
      case Success(result) =>
        result
      case Failure(exception) =>
        throw new RuntimeException(exception)
    }
  }
}
