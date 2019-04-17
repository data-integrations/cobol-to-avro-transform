/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.common;

import com.google.common.io.ByteSource;

import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link ByteSource} for the {@link InputStream}.
 */
public class StreamByteSource extends ByteSource {

  private final InputStream is;
  private final long length;

  public StreamByteSource(InputStream is, long length) {
    this.is = is;
    this.length = length;
  }

  @Override
  public InputStream openStream() throws IOException {
    return is;
  }

  @Override
  public long size() throws IOException {
    return length;
  }
}
