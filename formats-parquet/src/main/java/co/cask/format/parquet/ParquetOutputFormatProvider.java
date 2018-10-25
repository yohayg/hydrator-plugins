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

package co.cask.format.parquet;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Output format plugin for parquet.
 */
@Plugin(type = "outputformat")
@Name("parquet")
@Description("Parquet output format plugin that provides the output format class name and properties "
  + "required to write Parquet files.")
public class ParquetOutputFormatProvider implements OutputFormatProvider {
  static final String SCHEMA_KEY = "parquet.avro.schema";
  private static final String CODEC_SNAPPY = "SNAPPY";
  private static final String CODEC_GZIP = "GZIP";
  private static final String CODEC_LZO = "LZO";
  private static final String PARQUET_COMPRESSION = "parquet.compression";
  private final Conf conf;

  public ParquetOutputFormatProvider(Conf conf) {
    this.conf = conf;
  }

  @Override
  public String getOutputFormatClassName() {
    return StructuredParquetOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    conf.validate();
    Map<String, String> configuration = new HashMap<>();
    if (conf.schema != null) {
      configuration.put(SCHEMA_KEY, conf.schema);
    }

    if (conf.compressionCodec != null && !"none".equalsIgnoreCase(conf.compressionCodec)) {
      if (CODEC_SNAPPY.equalsIgnoreCase(conf.compressionCodec)) {
        configuration.put(PARQUET_COMPRESSION, CODEC_SNAPPY);
      } else if (CODEC_GZIP.equalsIgnoreCase(conf.compressionCodec)) {
        configuration.put(PARQUET_COMPRESSION, CODEC_GZIP);
      } else if (CODEC_LZO.equalsIgnoreCase(conf.compressionCodec)) {
        configuration.put(PARQUET_COMPRESSION, CODEC_LZO);
      } else {
        throw new IllegalArgumentException("Unsupported compression codec " + conf.compressionCodec);
      }
    }
    return configuration;
  }

  /**
   * Configuration for the output format plugin.
   */
  public static class Conf extends PluginConfig {

    @Macro
    @Nullable
    @Description("schema of the data to write.")
    private String schema;

    @Macro
    @Nullable
    @Description("compression codec to use when writing data. Must be 'snappy', 'gzip', 'lzo', or 'none'.")
    private String compressionCodec;

    private void validate() {
      if (!containsMacro("schema") && schema != null) {
        try {
          Schema.parseJson(schema);
        } catch (IOException e) {
          throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage(), e);
        }
      }
    }
  }
}
