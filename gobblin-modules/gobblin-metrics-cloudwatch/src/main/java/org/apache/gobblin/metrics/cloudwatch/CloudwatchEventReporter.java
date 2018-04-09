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

package org.apache.gobblin.metrics.cloudwatch;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.google.common.base.Optional;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;
import org.apache.gobblin.metrics.event.JobEvent;
import org.apache.gobblin.metrics.event.MultiPartEvent;
import org.apache.gobblin.metrics.event.TaskEvent;
import org.apache.gobblin.metrics.reporter.EventReporter;

import static org.apache.gobblin.metrics.event.JobEvent.METADATA_JOB_ID;
import static org.apache.gobblin.metrics.event.JobEvent.METADATA_JOB_STATE;
import static org.apache.gobblin.metrics.event.TaskEvent.METADATA_TASK_ID;
import static org.apache.gobblin.metrics.event.TimingEvent.METADATA_DURATION;


/**
 *
 * {@link org.apache.gobblin.metrics.reporter.EventReporter} that emits {@link org.apache.gobblin.metrics.GobblinTrackingEvent} events
 * as timestamped name - value pairs through the Graphite protocol
 *
 * @author Lorand Bendig
 *
 */
public class CloudwatchEventReporter extends EventReporter {

  private final CloudwatchPusher cloudwatchPusher;
  private final boolean emitValueAsKey;

  private static final Double EMTPY_VALUE = 0D;
  private static final Logger LOGGER = LoggerFactory.getLogger(CloudwatchEventReporter.class);
  private String prefix;

  public CloudwatchEventReporter(Builder<?> builder) throws IOException {
    super(builder);
    if (builder.cloudwatchPusher.isPresent()) {
      this.cloudwatchPusher = builder.cloudwatchPusher.get();
    } else {
      this.cloudwatchPusher =
          this.closer.register(new CloudwatchPusher());
    }
    this.emitValueAsKey = builder.emitValueAsKey;
    this.prefix = builder.prefix;
  }

  @Override
  public void reportEventQueue(Queue<GobblinTrackingEvent> queue) {

    GobblinTrackingEvent nextEvent;
    try {
      while (null != (nextEvent = queue.poll())) {
        pushEvent(nextEvent);
      }
      this.cloudwatchPusher.flush();
    } catch (IOException e) {
      LOGGER.error("Error sending event to Cloudwatch", e);
      try {
        this.cloudwatchPusher.flush();
      } catch (IOException e1) {
        LOGGER.error("Unable to flush previous events to Cloudwatch", e);
      }
    }
  }

  private List<Dimension> createDimensionsFromMetadata(Map<String, String> metadata) {
    LinkedList<Dimension> dimensions = new LinkedList<>();

    Dimension dimensionJobId = new Dimension();
    dimensionJobId.setName(METADATA_JOB_ID);
    dimensionJobId.setValue(metadata.get(METADATA_JOB_ID));
    dimensions.add(dimensionJobId);

    Dimension dimensionTaskId = new Dimension();
    dimensionTaskId.setName(METADATA_TASK_ID);
    dimensionTaskId.setValue(metadata.get(METADATA_TASK_ID));
    dimensions.add(dimensionTaskId);

    if (metadata.get(METADATA_JOB_STATE) != null) {
      Dimension dimensionState = new Dimension();
      dimensionState.setName(METADATA_JOB_STATE);
      dimensionState.setValue(metadata.get(METADATA_JOB_STATE));
      dimensions.add(dimensionState);
    }

    if (metadata.get(EventSubmitter.EVENT_TYPE) != null) {
      Dimension dimensionEventType = new Dimension();
      dimensionEventType.setName(EventSubmitter.EVENT_TYPE);
      dimensionEventType.setValue(metadata.get(EventSubmitter.EVENT_TYPE));
      dimensions.add(dimensionEventType);
    }


    return dimensions;
  }

  /**
   * Extracts the event and its metadata from {@link GobblinTrackingEvent} and creates
   * timestamped name value pairs
   *
   * @param event {@link GobblinTrackingEvent} to be reported
   * @throws IOException
   */
  private void pushEvent(GobblinTrackingEvent event) throws IOException {

    Map<String, String> metadata = event.getMetadata();

    String name = event.getName();
    long timestamp = event.getTimestamp();
    MultiPartEvent multipartEvent = MultiPartEvent.getEvent(metadata.get(GobblinEventBuilder.EVENT_TYPE));
    if (multipartEvent == null) {
      List<Dimension> dimensions = createDimensionsFromMetadata(metadata);
      cloudwatchPusher.push(JOINER.join(prefix, name), EMTPY_VALUE, timestamp, dimensions);
    }
    else {
      for (String field : multipartEvent.getMetadataFields()) {
        String value = metadata.get(field);
        List<Dimension> dimensions = createDimensionsFromMetadata(metadata);

        if (value == null) {
          cloudwatchPusher.push(JOINER.join(prefix, name, field), EMTPY_VALUE, timestamp, dimensions);
        } else {
          if (emitAsKey(field)) {
            // metric value is emitted as part of the keys
            cloudwatchPusher.push(JOINER.join(prefix, name, field, value), 1D, timestamp, dimensions);
          } else {
            cloudwatchPusher.push(JOINER.join(prefix, name, field), convertValue(field, value), timestamp, dimensions);
          }
        }
      }
    }
  }

  private Double convertValue(String field, String value) {
    return METADATA_DURATION.equals(field) ? convertDuration(TimeUnit.MILLISECONDS.toNanos(Long.parseLong(value)))
        : Double.parseDouble(value);
  }

  /**
   * Non-numeric event values may be emitted as part of the key by applying them to the end of the key if
   * {@link ConfigurationKeys#METRICS_REPORTING_GRAPHITE_EVENTS_VALUE_AS_KEY} is set. Thus such events can be still
   * reported even when the backend doesn't accept text values through Graphite
   *
   * @param field name of the metric's metadata fields
   * @return true if event value is emitted in the key
   */
  private boolean emitAsKey(String field) {
    return emitValueAsKey
        && (field.equals(TaskEvent.METADATA_TASK_WORKING_STATE) || field.equals(JobEvent.METADATA_JOB_STATE));
  }

  /**
   * Returns a new {@link CloudwatchEventReporter.Builder} for {@link CloudwatchEventReporter}.
   * Will automatically add all Context tags to the reporter.
   *
   * @param context the {@link org.apache.gobblin.metrics.MetricContext} to report
   * @return GraphiteEventReporter builder
   * @deprecated this method is bugged. Use {@link CloudwatchEventReporter.Factory#forContext} instead.
   */
  @Deprecated
  public static Builder<? extends Builder> forContext(MetricContext context) {
    return new BuilderImpl(context);
  }

  public static class BuilderImpl extends Builder<BuilderImpl> {

    private BuilderImpl(MetricContext context) {
      super(context);
    }

    @Override
    protected BuilderImpl self() {
      return this;
    }
  }

  public static class Factory {
    /**
     * Returns a new {@link CloudwatchEventReporter.Builder} for {@link CloudwatchEventReporter}.
     * Will automatically add all Context tags to the reporter.
     *
     * @param context the {@link org.apache.gobblin.metrics.MetricContext} to report
     * @return GraphiteEventReporter builder
     */
    public static BuilderImpl forContext(MetricContext context) {
      return new BuilderImpl(context);
    }
  }

  /**
   * Builder for {@link CloudwatchEventReporter}.
   * Defaults to no filter, reporting rates in seconds and times in milliseconds using TCP connection
   */
  public static abstract class Builder<T extends EventReporter.Builder<T>> extends EventReporter.Builder<T> {
    protected Optional<CloudwatchPusher> cloudwatchPusher;
    protected boolean emitValueAsKey;
    protected String prefix;

    protected Builder(MetricContext context) {
      super(context);
      this.cloudwatchPusher = Optional.absent();
    }

    /**
     * Set {@link org.apache.gobblin.metrics.cloudwatch.CloudwatchPusher} to use.
     */
    public T withCloudwatchPusher(CloudwatchPusher pusher) {
      this.cloudwatchPusher = Optional.of(pusher);
      return self();
    }


    public T withPrefix(String prefix) {
      this.prefix = prefix;
      return self();
    }

    /**
     * Set flag that forces the reporter to emit non-numeric event values as part of the key
     */
    public T withEmitValueAsKey(boolean emitValueAsKey) {
      this.emitValueAsKey = emitValueAsKey;
      return self();
    }

    /**
     * Builds and returns {@link CloudwatchEventReporter}.
     *
     * @return GraphiteEventReporter
     */
    public CloudwatchEventReporter build() throws IOException {
      return new CloudwatchEventReporter(this);
    }
  }

}
