package in.ashwanthkumar.scaldingdataflow;

import com.google.api.client.util.Lists;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PCtxWrapper<Input, Output> extends ScaldingContextWrapper<Input, Output> {
    private final static Logger LOG = LoggerFactory.getLogger(PCtxWrapper.class);

    private List<Output> outputs = Lists.newArrayList();

    public PCtxWrapper(DoFn<Input, Output> fn, Input element) {
        super(fn, element);
    }

    public Iterable<Output> getOutput() {
        return outputs;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
        // FIXME
        return null;
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
        // FIXME
        return null;
    }

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregatorInternal(String name, Combine.CombineFn<AggInputT, ?, AggOutputT> combiner) {
        // FIXME
        return null;
    }
}
