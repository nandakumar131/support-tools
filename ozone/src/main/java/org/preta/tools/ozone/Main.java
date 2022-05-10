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

package org.preta.tools.ozone;

import org.preta.tools.ozone.benchmark.Benchmark;
import org.preta.tools.ozone.metagen.MetaGen;
import picocli.CommandLine;

@CommandLine.Command(name="ozone-tools",
    description = "Ozone Developer Tools.",
    versionProvider = OzoneVersionProvider.class,
    subcommands = {
        MetaGen.class,
        Benchmark.class
    },
    mixinStandardHelpOptions = true,
    hidden = true)
public class Main implements Runnable {

  private final CommandLine commandLine;

  private static Main getInstance() {
    return new Main();
  }

  private Main() {
    this.commandLine = new CommandLine(this);
  }

  @Override
  public void run() {
    throw new CommandLine.ParameterException(commandLine, "Missing Command!");
  }

  private int execute(final String[] args) {
    return commandLine.execute(args);
  }

  public static void main(final String[] args) {
    System.exit(Main.getInstance().execute(args));
  }

}
