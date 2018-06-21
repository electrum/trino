/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.jdbc;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class DriverLogger
{
    private static final File LOG_FILE = new File("/tmp/presto-jdbc.log");

    private DriverLogger() {}

    public static void log(String message)
    {
        String line = format("%s %s\n", Instant.now(), message);

        try (OutputStream out = new FileOutputStream(LOG_FILE, true)) {
            out.write(line.getBytes(UTF_8));
        }
        catch (IOException e) {
            System.err.println("Failed to write log: " + e);
        }
    }
}
