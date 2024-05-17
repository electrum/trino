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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.plugin.tpch.TpchConnectorFactory.TPCH_SPLITS_PER_NODE;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestPythonFunctions
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        Session session = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        QueryRunner runner = new StandaloneQueryRunner(session);
        runner.installPlugin(new TpchPlugin());
        runner.createCatalog(TEST_CATALOG_NAME, "tpch", ImmutableMap.of(TPCH_SPLITS_PER_NODE, "1"));
        runner.installPlugin(new PythonFunctionsPlugin());

        assertions = new QueryAssertions(runner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testInlineFunctions()
    {
        assertThat(assertions.query(
                """
                WITH function my_func(x bigint)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'twice')
                AS $$
                def twice(x):
                    return x * 2
                $$
                SELECT my_func(nationkey)
                FROM nation
                WHERE nationkey = 21
                """))
                .matches("VALUES bigint '42'");
    }

    @Test
    public void testInvalidHandler()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'bad')
                AS $$
                def twice(x):
                    return x * 2
                $$
                SELECT my_func(13)
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage(
                        """
                        line 1:6: Failed to setup Python function:
                        AttributeError: module 'guest' has no attribute 'bad'
                        Cannot find function 'bad' in 'guest'
                        """.stripTrailing());
    }

    @Test
    public void testSyntaxError()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'twice')
                AS $$
                defxxx twice(x):
                    return x * 2
                $$
                SELECT my_func(13)
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage(
                        """
                        line 1:6: Failed to setup Python function:
                        File "/guest/guest.py", line 2
                            defxxx twice(x):
                                   ^^^^^
                        SyntaxError: invalid syntax
                        Failed to load Python module 'guest'
                        """.stripTrailing());
    }

    @Test
    public void testDivideByZero()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'divzero')
                AS $$
                def divzero(x):
                    return x / 0
                $$
                SELECT my_func(nationkey)
                FROM nation
                """))
                .failure()
                .hasErrorCode(DIVISION_BY_ZERO)
                .hasMessage("division by zero")
                .hasRootCauseMessage(
                        """
                        Python traceback:
                        Traceback (most recent call last):
                          File "/guest/guest.py", line 3, in divzero
                            return x / 0
                                   ~~^~~
                        ZeroDivisionError: division by zero
                        """.stripTrailing());
    }

    @Test
    public void testNotSupported()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'notsupported')
                AS $$
                from trino import *
                def notsupported(x):
                    raise TrinoError(NOT_SUPPORTED, "test not-supported")
                $$
                SELECT my_func(nationkey)
                FROM nation
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("test not-supported")
                .hasRootCauseMessage(
                        """
                         Python traceback:
                         Traceback (most recent call last):
                           File "/guest/guest.py", line 4, in notsupported
                             raise TrinoError(NOT_SUPPORTED, "test not-supported")
                         trino.TrinoError: test not-supported
                         """.stripTrailing());
    }

    @Test
    public void testNumericValueOutOfRange()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'outofrange')
                AS $$
                from trino import *
                def outofrange(x):
                    raise NumericValueOutOfRangeError("test out-of-range")
                $$
                SELECT my_func(nationkey)
                FROM nation
                """))
                .failure()
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("test out-of-range")
                .hasRootCauseMessage(
                        """
                         Python traceback:
                         Traceback (most recent call last):
                           File "/guest/guest.py", line 4, in outofrange
                             raise NumericValueOutOfRangeError("test out-of-range")
                         trino.NumericValueOutOfRangeError: test out-of-range
                         """.stripTrailing());
    }

    @Test
    public void testInvalidFunctionArgument()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'badArg')
                AS $$
                from trino import *
                def badArg(x):
                    raise InvalidFunctionArgumentError("test bad-arg")
                $$
                SELECT my_func(nationkey)
                FROM nation
                """))
                .failure()
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("test bad-arg")
                .hasRootCauseMessage(
                        """
                         Python traceback:
                         Traceback (most recent call last):
                           File "/guest/guest.py", line 4, in badArg
                             raise InvalidFunctionArgumentError("test bad-arg")
                         trino.InvalidFunctionArgumentError: test bad-arg
                         """.stripTrailing());
    }

    @Test
    public void testGenericException()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'other')
                AS $$
                from trino import *
                def other(x):
                    raise ValueError("test other")
                $$
                SELECT my_func(nationkey)
                FROM nation
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("ValueError: test other")
                .hasRootCauseMessage(
                        """
                         Python traceback:
                         Traceback (most recent call last):
                           File "/guest/guest.py", line 4, in other
                             raise ValueError("test other")
                         ValueError: test other
                         """.stripTrailing());
    }

    @Test
    public void testReturnValueConversionError()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(s varchar)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'oops')
                AS $$
                def oops(s):
                    return s
                $$
                SELECT my_func(comment)
                FROM nation
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result (str) to Trino type (BIGINT): TypeError: 'str' object cannot be interpreted as an integer");
    }

    @Test
    public void testSplitWords()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION reverse_words(s varchar)
                RETURNS varchar
                LANGUAGE PYTHON
                WITH (handler = 'reverse_words')
                AS $$
                import re

                def reverse(s):
                    str = ""
                    for i in s:
                        str = i + str
                    return str

                pattern = re.compile(r"\\w+[.,'!?\\"]\\w*")

                def process_word(word):
                    # Reverse only words without non-letter signs
                    return word if pattern.match(word) else reverse(word)

                def reverse_words(payload):
                    text_words = payload.split(' ')
                    return ' '.join([process_word(w) for w in text_words])
                $$
                SELECT comment, reverse_words(comment)
                FROM nation
                WHERE nationkey IN (5, 6, 12)
                """))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                            ('ven packages wake quickly. regu', 'nev segakcap ekaw quickly. uger'),
                            ('refully final requests. regular, ironi', 'yllufer lanif requests. regular, inori'),
                            ('ously. final, express gifts cajole a', 'ously. final, sserpxe stfig elojac a')
                        """);
    }

    @Test
    public void testTypeBoolean()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION xor(a boolean, b boolean)
                RETURNS boolean
                LANGUAGE PYTHON
                WITH (handler = 'xor')
                AS $$
                import operator
                def xor(a, b):
                    return operator.xor(a, b)
                $$
                SELECT xor(false, false), xor(false, true), xor(true, false), xor(true, true)
                """))
                .skippingTypesCheck()
                .matches("VALUES (false, true, true, false)");
    }

    @Test
    public void testTypeShortDecimal()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION test_decimal_short(x decimal(18, 5))
                RETURNS decimal(18, 5)
                LANGUAGE PYTHON
                WITH (handler = 'square')
                AS $$
                def square(x):
                    return x * x
                $$
                VALUES test_decimal_short(123.456)
                """))
                .skippingTypesCheck()
                .matches("VALUES CAST(15241.38394 AS DECIMAL(18, 5))");
    }

    @Test
    public void testTypeLongDecimal()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION test_decimal_long(x decimal(38, 5))
                RETURNS decimal(38, 5)
                LANGUAGE PYTHON
                WITH (handler = 'square')
                AS $$
                def square(x):
                    return x * x
                $$
                VALUES test_decimal_long(123.456)
                """))
                .skippingTypesCheck()
                .matches("VALUES CAST(15241.38394 AS DECIMAL(38, 5))");
    }
}
