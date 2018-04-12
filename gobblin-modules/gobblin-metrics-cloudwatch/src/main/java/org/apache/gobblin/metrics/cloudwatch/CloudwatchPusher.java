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


/**
 * Establishes a connection through the Graphite protocol and pushes timestamped name - value pairs
 *
 * @author Lorand Bendig
 *
 */
public class CloudwatchPusher implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(CloudwatchPusher.class);
  public static final String LOG_GROUP_NAME = "prezi/data/gobblin";
  public static final String LOG_STREAM_NAME = "gobblin1";

  private LinkedBlockingQueue<InputLogEvent> events = new LinkedBlockingQueue<>();

  private AWSLogs awsLogClient;

  private String sequenceToken;

  private int EVENT_BATCH_SIZE = 10;

  public CloudwatchPusher() throws IOException {
    boolean isCreateLogStream = true;
    if (this.awsLogClient == null) {
      this.awsLogClient = AWSLogsClient.builder().build();
      DescribeLogStreamsRequest describeLogStreamsRequest = new DescribeLogStreamsRequest();
      describeLogStreamsRequest.setLogGroupName(LOG_GROUP_NAME);
      describeLogStreamsRequest.setLogStreamNamePrefix(LOG_STREAM_NAME);
      DescribeLogStreamsResult res = awsLogClient.describeLogStreams(describeLogStreamsRequest);

      if (res.getLogStreams().size() == 0 && isCreateLogStream) {
        CreateLogStreamRequest logStreamRequest = new CreateLogStreamRequest();
        logStreamRequest.setLogGroupName(LOG_GROUP_NAME);
        logStreamRequest.setLogStreamName(LOG_STREAM_NAME);

        awsLogClient.createLogStream(logStreamRequest);
      } else if (res.getLogStreams().size() == 0) {
        throw new RuntimeException("LogStream " + LOG_STREAM_NAME + " does not exists");
      }

      this.sequenceToken = res.getLogStreams().get(0).getUploadSequenceToken();

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

  public synchronized void flush() throws IOException {
    LinkedList<InputLogEvent> eventsToSend = new LinkedList<>();
    long numberOfRecords = events.drainTo(eventsToSend, EVENT_BATCH_SIZE);

    if (numberOfRecords > 0 ) {
      PutLogEventsRequest request = new PutLogEventsRequest();
      request.setLogEvents(eventsToSend);
      request.setLogGroupName(LOG_GROUP_NAME);
      request.setLogStreamName(LOG_STREAM_NAME);
      request.setSequenceToken(this.sequenceToken);

      PutLogEventsResult result = this.awsLogClient.putLogEvents(request);
      this.sequenceToken = result.getNextSequenceToken();

      LOGGER.debug("{} metric was pushed to Cloudwatch and {} failed",numberOfRecords);
    }else {
      LOGGER.debug("No metric to push to Cloudwatch");
    }


    if (events.size() > 0) {
      flush();
    }
  }

  @Override
  public void close() throws IOException {
    this.awsLogClient.shutdown();
  }

}
