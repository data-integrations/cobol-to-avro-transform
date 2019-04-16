/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.common;

import com.google.common.io.ByteSource;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link ByteSource} for {@link Location}.
 */
public class LocationByteSource extends ByteSource {

  private final Location location;

  public LocationByteSource(Location location) {
    this.location = location;
  }

  @Override
  public InputStream openStream() throws IOException {
    return location.getInputStream();
  }

  @Override
  public long size() throws IOException {
    return location.length();
  }
}
