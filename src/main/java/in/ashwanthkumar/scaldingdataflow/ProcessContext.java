package in.ashwanthkumar.scaldingdataflow;

import com.google.api.client.util.Lists;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ProcessContext<Input, Output> extends ScaldingContextWrapper<Input, Output> {
    private final static Logger LOG = LoggerFactory.getLogger(ProcessContext.class);

    private List<Output> outputs = Lists.newArrayList();
    private Map<TupleTag<?>, Iterable<WindowedValue<?>>> sideInputs;
    private RuntimeContext context;

    public ProcessContext(DoFn<Input, Output> fn, Input element, Map<TupleTag<?>, Iterable<WindowedValue<?>>> sideInputs, RuntimeContext options) {
        super(fn, element);
        this.sideInputs = sideInputs;
        this.context = options;
    }

    public Iterable<Output> getOutput() {
        return outputs;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
        return context.pipelineOptions();
    }

    @Override
    public void output(Output output) {
        this.outputs.add(output);
    }

    @Override
    public void outputWithTimestamp(Output output, Instant timestamp) {
        output(output);
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
        return view.fromIterableInternal(sideInputs.get(view.getTagInternal()));
    }

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregatorInternal(String name, Combine.CombineFn<AggInputT, ?, AggOutputT> combiner) {
        // FIXME
        return null;
    }
}
