package in.ashwanthkumar.scaldingdataflow;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.util.TimerInternals;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingInternals;
import com.google.cloud.dataflow.sdk.util.state.StateInternals;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.Lists;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

abstract public class ScaldingContextWrapper<Input, Output> extends DoFn<Input, Output>.ProcessContext {
    private final static Logger LOG = LoggerFactory.getLogger(ScaldingContextWrapper.class);

    protected DoFn<Input, Output> fn;
    protected Input element;

    public ScaldingContextWrapper(DoFn<Input, Output> fn, Input element) {
        fn.super();
        this.fn = fn;
        this.element = element;
    }

    @Override
    public Input element() {
        return element;
    }

    @Override
    public Instant timestamp() {
        return Instant.now();
    }

    @Override
    public BoundedWindow window() {
        return GlobalWindow.INSTANCE;
    }

    @Override
    public PaneInfo pane() {
        return PaneInfo.NO_FIRING;
    }

    abstract public void output(Output output);

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {
        String message = "sideOutput is an unsupported operation for doFunctions, use a " +
                "MultiDoFunction instead.";
        LOG.warn(message);
        throw new UnsupportedOperationException(message);
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
        String message =
                "sideOutputWithTimestamp is an unsupported operation for doFunctions, use a " +
                        "MultiDoFunction instead.";
        LOG.warn(message);
        throw new UnsupportedOperationException(message);
    }

    @Override
    public void outputWithTimestamp(Output output, Instant timestamp) {
        output(output);
    }

    @Override
    public WindowingInternals<Input, Output> windowingInternals() {
        return new WindowingInternals<Input, Output>() {
            @Override
            public StateInternals stateInternals() {
                throw new UnsupportedOperationException(
                        "WindowingInternals#stateInternals() is not yet supported.");
            }

            @Override
            public void outputWindowedValue(Output output, Instant timestamp, Collection<? extends BoundedWindow> windows, PaneInfo pane) {
                output(output);
            }

            @Override
            public TimerInternals timerInternals() {
                throw new UnsupportedOperationException(
                        "WindowingInternals#timerInternals() is not yet supported.");
            }

            @Override
            public Collection<? extends BoundedWindow> windows() {
                return Lists.newArrayList(GlobalWindow.INSTANCE);
            }

            @Override
            public PaneInfo pane() {
                return PaneInfo.NO_FIRING;
            }

            @Override
            public <T> void writePCollectionViewData(TupleTag<?> tag, Iterable<WindowedValue<T>> data, Coder<T> elemCoder) throws IOException {
                throw new UnsupportedOperationException(
                        "WindowingInternals#writePCollectionViewData() is not yet supported.");
            }
        };
    }
}
