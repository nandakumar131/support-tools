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

package org.preta.tools.ozone.benchmark;

import org.preta.tools.ozone.OzoneVersionProvider;
import org.preta.tools.ozone.benchmark.ozone.OzoneWriteBenchmark;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name="ozone",
    description = "Tool to benchmark Ozone.",
    versionProvider = OzoneVersionProvider.class,
    mixinStandardHelpOptions = true,
    subcommands = {
        OzoneWriteBenchmark.class}
        )
public class OzoneBenchmark implements Runnable {

  private final CommandLine commandLine = new CommandLine(this);

  private static OzoneBenchmark getInstance() {
    return new OzoneBenchmark();
  }

  private int execute(String[] args) {
    return commandLine.execute(args);
  }

  public void run() {
    throw new CommandLine.ParameterException(commandLine, "Missing SubCommand!");
  }

  public static void main(String[] args) {
    System.exit(getInstance().execute(args));
  }

}
