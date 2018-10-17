/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package org.apache.gobblin.writer;

import java.io.IOException;


/**
 * A {@link DataWriterBuilder} for building {@link DataWriter} that writes bytes.
 *
 * @author akshay@nerdwallet.com
 */
public class SimpleCompressionDataWriterBuilder extends FsDataWriterBuilder<String, byte[]> {
  /**
   * Build a {@link DataWriter}.
   *
   * @return the built {@link DataWriter}
   * @throws IOException if there is anything wrong building the writer
   */
  @Override
  public DataWriter<byte[]> build() throws IOException {
    return new SimpleCompressionDataWriter(this, this.destination.getProperties());
  }

}
