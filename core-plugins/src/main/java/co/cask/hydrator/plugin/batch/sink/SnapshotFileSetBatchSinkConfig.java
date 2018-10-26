/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.hydrator.common.TimeParser;
import co.cask.hydrator.plugin.common.SnapshotFileSetConfig;

import javax.annotation.Nullable;

/**
 * Config for SnapshotFileBatchSink
 */
public class SnapshotFileSetBatchSinkConfig extends SnapshotFileSetConfig {
  @Description("Optional property that configures the sink to delete old partitions after successful runs. " +
    "If set, when a run successfully finishes, the sink will subtract this amount of time from the runtime and " +
    "delete any partitions older than that time. " +
    "The format is expected to be a number followed by an 's', 'm', 'h', or 'd' specifying the time unit, with 's' " +
    "for seconds, 'm' for minutes, 'h' for hours, and 'd' for days. For example, if the pipeline is scheduled to " +
    "run at midnight of January 1, 2016, and this property is set to 7d, the sink will delete any partitions " +
    "for time partitions older than midnight Dec 25, 2015.")
  @Nullable
  @Macro
  protected String cleanPartitionsOlderThan;

  public SnapshotFileSetBatchSinkConfig() {

  }

  public SnapshotFileSetBatchSinkConfig(String name, @Nullable String basePath,
                                        @Nullable String cleanPartitionsOlderThan) {
    super(name, basePath, null);
    this.cleanPartitionsOlderThan = cleanPartitionsOlderThan;
  }

  public String getCleanPartitionsOlderThan() {
    return cleanPartitionsOlderThan;
  }

  public void validate() {
    if (cleanPartitionsOlderThan != null) {
      TimeParser.parseDuration(cleanPartitionsOlderThan);
    }
  }
}
