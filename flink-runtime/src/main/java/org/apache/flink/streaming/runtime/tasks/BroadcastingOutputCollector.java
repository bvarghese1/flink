/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.XORShiftRandom;

import java.util.Random;

class BroadcastingOutputCollector<T> implements WatermarkGaugeExposingOutput<StreamRecord<T>> {

    protected final OutputWithChainingCheck<StreamRecord<T>>[] outputs;
    private final Random random = new XORShiftRandom();
    private final WatermarkGauge watermarkGauge = new WatermarkGauge();
    protected final Counter numRecordsOutForTask;

    public BroadcastingOutputCollector(
            OutputWithChainingCheck<StreamRecord<T>>[] outputs, Counter numRecordsOutForTask) {
        this.outputs = outputs;
        this.numRecordsOutForTask = numRecordsOutForTask;
    }

    @Override
    public void emitWatermark(Watermark mark) {
        watermarkGauge.setCurrentWatermark(mark.getTimestamp());
        for (Output<StreamRecord<T>> output : outputs) {
            output.emitWatermark(mark);
        }
    }

    @Override
    public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
        for (Output<StreamRecord<T>> output : outputs) {
            output.emitWatermarkStatus(watermarkStatus);
        }
    }

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) {
        if (outputs.length <= 0) {
            // ignore
        } else if (outputs.length == 1) {
            outputs[0].emitLatencyMarker(latencyMarker);
        } else {
            // randomly select an output
            outputs[random.nextInt(outputs.length)].emitLatencyMarker(latencyMarker);
        }
    }

    @Override
    public Gauge<Long> getWatermarkGauge() {
        return watermarkGauge;
    }

    @Override
    public void collect(StreamRecord<T> record) {
        boolean emitted = false;
        for (OutputWithChainingCheck<StreamRecord<T>> output : outputs) {
            emitted |= output.collectAndCheckIfChained(record);
        }
        if (emitted) {
            numRecordsOutForTask.inc();
        }
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        boolean emitted = false;
        for (OutputWithChainingCheck<StreamRecord<T>> output : outputs) {
            emitted |= output.collectAndCheckIfChained(outputTag, record);
        }
        if (emitted) {
            numRecordsOutForTask.inc();
        }
    }

    @Override
    public void close() {
        for (Output<StreamRecord<T>> output : outputs) {
            output.close();
        }
    }

    @Override
    public void emitRecordAttributes(RecordAttributes recordAttributes) {
        for (OutputWithChainingCheck<StreamRecord<T>> output : outputs) {
            output.emitRecordAttributes(recordAttributes);
        }
    }

    @Override
    public void emitWatermark(WatermarkEvent watermark) {
        for (OutputWithChainingCheck<StreamRecord<T>> output : outputs) {
            output.emitWatermark(watermark);
        }
    }
}
