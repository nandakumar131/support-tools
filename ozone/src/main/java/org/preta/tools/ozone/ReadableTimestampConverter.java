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

import picocli.CommandLine.ITypeConverter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * String to time duration converter for use with PicoCLI.
 *
 * Parse a time with human readable suffix e.g. 10s, 2m, 4h, 6d.
 * The default unit is seconds, assuming there is no suffix.
 */
public class ReadableTimestampConverter implements ITypeConverter<Long> {
  private static final Pattern timePattern = Pattern.compile("^(\\d+)([smhdw]?)$");

  /**
   * Convert time duration string to value in seconds.
   * @param time time in human readable format
   * @return time duration in seconds
   */
  @Override
  public Long convert(String time) {
    Matcher matcher = timePattern.matcher(time.toLowerCase());
    long multiplier = 1;
    if (matcher.find()) {
      if (matcher.group(2).length() > 0) {
        switch (matcher.group(2).toLowerCase().charAt(0)) {
          case 'w':
          multiplier *= 7;
          case 'd':
          multiplier *= 24;
          case 'h':
          multiplier *= 60;
          case 'm':
          multiplier *= 60;
          case 's':
          break;
        }
      }
      return Long.parseLong(matcher.group(1)) * multiplier;
    }
    throw new IllegalArgumentException("Unrecognized time format " + time);
  }
}