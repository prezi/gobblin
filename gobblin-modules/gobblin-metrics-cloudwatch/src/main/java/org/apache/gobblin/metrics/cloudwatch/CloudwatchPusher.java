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

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.model.CreateLogStreamRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.PutLogEventsRequest;
import com.amazonaws.services.logs.model.PutLogEventsResult;

import org.apache.gobblin.runtime.locks.JobLock;
import org.apache.gobblin.runtime.locks.JobLockException;


/**
 * Establishes a connection through the Graphite protocol and pushes timestamped name - value pairs
 *
 * @author Lorand Bendig
 *
 */
public class CloudwatchPusher implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(CloudwatchPusher.class);

  private LinkedBlockingQueue<InputLogEvent> events = new LinkedBlockingQueue<>();

  private AWSLogs awsLogClient;

  private int EVENT_BATCH_SIZE = 1000;
  private String _logGroupName;
  private String _logStreamName;
  private boolean _isCreateLogStream;
  private JobLock lock;

  public CloudwatchPusher(String logGroupName, String logStreamName, boolean isCreateLogStream, JobLock lock) throws IOException {
    this._logGroupName = logGroupName;
    this._logStreamName = logStreamName;
    this._isCreateLogStream = isCreateLogStream;
    this.lock = lock;
    LOGGER.info("Using CloudwatchPusher with LogGroupName: {}, LogStreamName: {}, isCreateLogStream: {}", this._logGroupName, this._logGroupName, this._isCreateLogStream);
    if (this.awsLogClient == null) {
      this.awsLogClient = AWSLogsClient.builder().build();
      DescribeLogStreamsRequest describeLogStreamsRequest = new DescribeLogStreamsRequest();
      describeLogStreamsRequest.setLogGroupName(this._logGroupName);
      describeLogStreamsRequest.setLogStreamNamePrefix(this._logStreamName);

      DescribeLogStreamsResult res = awsLogClient.describeLogStreams(describeLogStreamsRequest);

      if (res.getLogStreams().size() == 0 && this._isCreateLogStream) {
        CreateLogStreamRequest logStreamRequest = new CreateLogStreamRequest();
        logStreamRequest.setLogGroupName(this._logGroupName);
        logStreamRequest.setLogStreamName(this._logStreamName);

        awsLogClient.createLogStream(logStreamRequest);

      } else if (res.getLogStreams().size() == 0) {
        throw new RuntimeException("LogStream " + this._logStreamName + " does not exists");
      }
    }
  }

  /**
   * Pushes a single metrics through
   *
   * @param timestamp associated timestamp
   * @param event is the json representation of the event
   * @throws IOException
   */
  public void push(long timestamp, String eventType, String event) throws IOException {

    InputLogEvent inputLog = new InputLogEvent()
        .withTimestamp(timestamp)
        .withMessage(event);

    events.add(inputLog);
    LOGGER.info("Sending log: {}", inputLog);

  }

  public String getSequenceToken() {
    DescribeLogStreamsRequest describeLogStreamsRequest = new DescribeLogStreamsRequest();
    describeLogStreamsRequest.setLogGroupName(this._logGroupName);
    describeLogStreamsRequest.setLogStreamNamePrefix(this._logStreamName);

    DescribeLogStreamsResult res = awsLogClient.describeLogStreams(describeLogStreamsRequest);
    return res.getLogStreams().get(0).getUploadSequenceToken();
  }

  public synchronized void flush()
      throws IOException, JobLockException {
    if (!this.lock.isLocked()) {
      if (!this.lock.tryLock()) {
        throw new RuntimeException("Unable to get lock for metric lock");
      }
    }
    try {
      LinkedList<InputLogEvent> eventsToSend = new LinkedList<>();
      long numberOfRecords = events.drainTo(eventsToSend, EVENT_BATCH_SIZE);
      eventsToSend.sort(Comparator.comparing(InputLogEvent::getTimestamp));

      if (numberOfRecords > 0) {
        PutLogEventsRequest request = new PutLogEventsRequest();
        request.setLogEvents(eventsToSend);
        request.setLogGroupName(this._logGroupName);
        request.setLogStreamName(this._logStreamName);
        request.setSequenceToken(getSequenceToken());

        PutLogEventsResult result = this.awsLogClient.putLogEvents(request);
        LOGGER.debug("{} metric was pushed to Cloudwatch and {} failed", numberOfRecords);
      } else {
        LOGGER.debug("No metric to push to Cloudwatch");
      }

      if (events.size() > 0) {
        flush();
      }
    } finally{
      if (this.lock.isLocked()) {
        this.lock.unlock();
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (this.lock.isLocked()) {
        this.lock.unlock();
      }
    } catch (JobLockException e) {
      LOGGER.error(e.getMessage());
      throw new RuntimeException(e);
    }
    this.awsLogClient.shutdown();
  }
}
