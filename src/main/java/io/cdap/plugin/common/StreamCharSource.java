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

import com.google.common.io.CharSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

/**
 * {@link CharSource} for the {@link InputStream}
 */
public class StreamCharSource extends CharSource {
  private final InputStream is;
  public StreamCharSource(InputStream is) {
    this.is = is;
  }
  @Override
  public Reader openStream() throws IOException {
    return new InputStreamReader(is, StandardCharsets.UTF_8);
  }
}
