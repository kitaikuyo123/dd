package com.minisql.stress;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.*;

public class LatencyRecorder {

    private final ConcurrentHashMap<Long, LongAdder> histogram = new ConcurrentHashMap<>();
    private final LongAdder totalCount = new LongAdder();
    private final AtomicLong minMicros = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxMicros = new AtomicLong(Long.MIN_VALUE);
    private final LongAdder totalMicros = new LongAdder();

    public void record(long durationMicros) {
        histogram.computeIfAbsent(durationMicros, k -> new LongAdder()).increment();
        totalCount.increment();
        totalMicros.add(durationMicros);
        minMicros.updateAndGet(cur -> Math.min(cur, durationMicros));
        maxMicros.updateAndGet(cur -> Math.max(cur, durationMicros));
    }

    public long getCount() { return totalCount.sum(); }

    public long getMin() { return minMicros.get() == Long.MAX_VALUE ? 0 : minMicros.get(); }

    public long getMax() { return maxMicros.get() == Long.MIN_VALUE ? 0 : maxMicros.get(); }

    public double getMean() {
        long count = getCount();
        return count == 0 ? 0 : (double) totalMicros.sum() / count;
    }

    public double getPercentile(double percentile) {
        long count = getCount();
        if (count == 0) return 0;

        long target = (long) (count * percentile);
        List<Map.Entry<Long, LongAdder>> sorted = new ArrayList<>(histogram.entrySet());
        sorted.sort(Map.Entry.comparingByKey());

        long accumulated = 0;
        for (Map.Entry<Long, LongAdder> entry : sorted) {
            accumulated += entry.getValue().sum();
            if (accumulated >= target) {
                return entry.getKey();
            }
        }
        return sorted.get(sorted.size() - 1).getKey();
    }
}
