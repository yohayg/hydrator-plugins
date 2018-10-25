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

package co.cask.format.delimited;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.plugin.PluginConfig;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Output format plugin for delimited.
 */
@Plugin(type = "outputformat")
@Name("delimited")
@Description("Delimited output format plugin that provides the output format class name and properties "
  + "required to write delimited text files.")
public class DelimitedOutputFormatProvider implements OutputFormatProvider {
  private final Conf conf;

  public DelimitedOutputFormatProvider(Conf conf) {
    this.conf = conf;
  }

  @Override
  public String getOutputFormatClassName() {
    return StructuredDelimitedOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    if (conf.containsMacro("delimiter")) {
      return Collections.emptyMap();
    }
    return StructuredDelimitedOutputFormat.getConfiguration(conf.delimiter);
  }

  /**
   * Configuration for the delimited format.
   */
  public static class Conf extends PluginConfig {
    @Macro
    @Nullable
    @Description("The delimiter to use to separate record fields.")
    private String delimiter;

    public Conf() {
      delimiter = ",";
    }
  }
}
