/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.annotation.Requirements;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.hydrator.format.FileFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.annotation.Nullable;


/**
 * {@link SnapshotFileBatchSink} that stores data in text format.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("SnapshotText")
@Description("Sink for a SnapshotFileSet that writes data in Text format.")
@Requirements(datasetTypes = PartitionedFileSet.TYPE)
public class SnapshotFileBatchTextSink extends SnapshotFileBatchSink<SnapshotFileBatchTextSink.Conf> {
  private Conf config;

  public SnapshotFileBatchTextSink(Conf config) {
    super(config);
    this.config = config;
  }

  @Override
  protected String getOutputFormatPlugin() {
    return FileFormat.DELIMITED.name().toLowerCase();
  }

  @Override
  protected void addFileProperties(FileSetProperties.Builder propertiesBuilder) {
    propertiesBuilder
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setExploreFormat("text")
      .setExploreSchema("text string");
  }

  /**
   * Config for SnapshotFileBatchTextSink.
   */
  public static class Conf extends SnapshotFileSetBatchSinkConfig {
    @Description("The Delimiter used to combine the Structured Record fields, Defaults to tab")
    @Nullable
    @Macro
    private String delimiter;

    public Conf() {
      this.delimiter = "\t";
    }

  }
}
