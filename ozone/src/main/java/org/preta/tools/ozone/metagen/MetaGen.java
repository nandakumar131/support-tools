/*
 * Copyright 2019 Nandakumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.preta.tools.ozone.metagen;

import org.preta.tools.ozone.OzoneVersionProvider;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParameterException;

@Command(name="metagen",
    description = "Tool to generate Ozone metadata.",
    versionProvider = OzoneVersionProvider.class,
    subcommands = {
        OmMetaGen.class
    },
    mixinStandardHelpOptions = true)
public class MetaGen implements Runnable {

  private final CommandLine commandLine;

  private static MetaGen getInstance() {
    return new MetaGen();
  }

  private MetaGen() {
    this.commandLine = new CommandLine(this);
  }

  @Override
  public void run() {
    throw new ParameterException(commandLine, "Missing SubCommand!");
  }

  private int execute(final String[] args) {
    return commandLine.execute(args);
  }

  public static void main(final String[] args) {
    System.exit(MetaGen.getInstance().execute(args));
  }

}
