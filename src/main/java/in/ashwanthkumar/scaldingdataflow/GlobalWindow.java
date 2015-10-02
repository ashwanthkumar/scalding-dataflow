package in.ashwanthkumar.scaldingdataflow;

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class GlobalWindow extends BoundedWindow {
    public static final GlobalWindow INSTANCE = new GlobalWindow();

    // Triggers use maxTimestamp to set timers' timestamp. Timers fires when
    // the watermark passes their timestamps. So, the maxTimestamp needs to be
    // smaller than the TIMESTAMP_MAX_VALUE.
    // One standard day is subtracted from TIMESTAMP_MAX_VALUE to make sure
    // the maxTimestamp is smaller than TIMESTAMP_MAX_VALUE even after rounding up
    // to seconds or minutes.
    private static final Instant END_OF_GLOBAL_WINDOW =
            TIMESTAMP_MAX_VALUE.minus(Duration.standardDays(1));

    @Override
    public Instant maxTimestamp() {
        return END_OF_GLOBAL_WINDOW;
    }

    private GlobalWindow() {}

}
