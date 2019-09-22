/*
 * Copyright 2019 Nanda kumar
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
import picocli.CommandLine;



@CommandLine.Command(name="benchmark",
    description = "Tool to benchmark Ozone services.",
    versionProvider = OzoneVersionProvider.class,
    subcommands = {
    BenchmarkOM.class
    },
    mixinStandardHelpOptions = true)
public class Benchmark implements Runnable {

  private final CommandLine commandLine;

  private static Benchmark getInstance() {
    return new Benchmark();
  }

  private Benchmark() {
    this.commandLine = new CommandLine(this);
  }

  @Override
  public void run() {
    throw new CommandLine.ParameterException(commandLine, "Missing SubCommand!");
  }

  private int execute(final String[] args) {
    return commandLine.execute(args);
  }

  public static void main(final String[] args) {
    System.exit(Benchmark.getInstance().execute(args));
  }

}
