package me.geales.flink.eventproducer;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class EventProducerSourceFunction implements SourceFunction<Map<String, String>> {

    private volatile boolean isRunning = true;
    private final AtomicLong counter = new AtomicLong();

    @Override
    public void run(SourceContext<Map<String, String>> sourceContext) throws Exception {
        while(this.isRunning) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                System.err.print("Cancelled");
            }
            HashMap<String, String> map = new HashMap<>();
            map.put("id", Long.toString(this.counter.getAndIncrement()));
            map.put("type", "A");
            map.put("generatedAt", Long.toString((Instant.now().toEpochMilli())));
            sourceContext.collectWithTimestamp(map, Instant.now().toEpochMilli());
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
