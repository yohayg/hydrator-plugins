/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.EndpointPluginContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.format.FileFormat;
import co.cask.hydrator.format.input.PathTrackingInputFormat;
import co.cask.hydrator.format.input.TextInputProvider;
import co.cask.hydrator.format.plugin.AbstractFileSource;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import javax.ws.rs.Path;


/**
 * A {@link BatchSource} to use any distributed file system as a Source.
 */
@Plugin(type = "batchsource")
@Name("File")
@Description("Batch source for File Systems")
public class FileBatchSource extends AbstractFileSource<FileSourceConfig> {
  public static final Schema DEFAULT_SCHEMA = TextInputProvider.getDefaultSchema(null);
  static final String INPUT_NAME_CONFIG = "input.path.name";
  static final String INPUT_REGEX_CONFIG = "input.path.regex";
  static final String LAST_TIME_READ = "last.time.read";
  static final String CUTOFF_READ_TIME = "cutoff.read.time";
  static final String USE_TIMEFILTER = "timefilter";
  private final FileSourceConfig config;

  public FileBatchSource(FileSourceConfig config) {
    super(config);
    this.config = config;
  }

  /**
   * Endpoint method to get the output schema of a source.
   *
   * @param config configuration for the source
   * @param pluginContext context to create plugins
   * @return schema of fields
   */
  @Path("getSchema")
  public Schema getSchema(FileSourceConfig config, EndpointPluginContext pluginContext) {
    FileFormat fileFormat = config.getFormat();
    if (fileFormat == null) {
      return config.getSchema();
    }
    Schema schema = fileFormat.getSchema(config.getPathField());
    return schema == null ? config.getSchema() : schema;
  }

  @Override
  protected Map<String, String> getFileSystemProperties(BatchSourceContext context) {
    Map<String, String> properties = new HashMap<>(config.getFileSystemProperties());
    if (config.shouldCopyHeader()) {
      properties.put(PathTrackingInputFormat.COPY_HEADER, "true");
    }

    Pattern pattern = config.getFilePattern();
    properties.put(INPUT_REGEX_CONFIG, pattern == null ? ".*" : pattern.toString());
    properties.put("mapreduce.input.pathFilter.class", BatchFileFilter.class.getName());

    return properties;
  }
}
