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
package io.trino.plugin.functions.python;

enum TrinoType
{
    ROW(0),
    ARRAY(1),
    MAP(2),
    BOOLEAN(3),
    BIGINT(4),
    INTEGER(5),
    SMALLINT(6),
    TINYINT(7),
    DOUBLE(8),
    REAL(9),
    DECIMAL(10),
    VARCHAR(11),
    CHAR(12),
    VARBINARY(13),
    DATE(14),
    TIME(15),
    TIME_WITH_TIME_ZONE(16),
    TIMESTAMP(17),
    TIMESTAMP_WITH_TIME_ZONE(18),
    INTERVAL_YEAR_TO_MONTH(19),
    INTERVAL_DAY_TO_SECOND(20),
    JSON(21),
    UUID(22),
    IPADDRESS(23),
    /**/;

    private final int id;

    TrinoType(int id)
    {
        this.id = id;
    }

    public int id()
    {
        return id;
    }
}
