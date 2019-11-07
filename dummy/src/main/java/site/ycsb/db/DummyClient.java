/**
 * Copyright (c) 2013 - 2016 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db;

import site.ycsb.*;

import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Vector;

/**
 * A class that wraps the DummyClient to allow it to be interfaced with YCSB.
 * This class extends {@link DB} and implements the database interface used by YCSB client.
 */
public class DummyClient extends DB {

  @Override public void init() throws DBException {
  }

  @Override public void cleanup() {
  }

  @Override public Status read(final String table, final String key, final Set<String> fields,
      final Map<String, ByteIterator> result) {
    return Status.OK;
  }

  @Override public Status scan(final String table, final String startkey, final int recordcount,
      final Set<String> fields, final Vector<HashMap<String, ByteIterator>> result) {
    return Status.OK;
  }

  @Override public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    return Status.OK;
  }

  @Override public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    return Status.OK;
  }

  @Override public Status delete(final String table, final String key) {
    return Status.OK;
  }
}
