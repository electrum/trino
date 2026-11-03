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
package io.trino.tests.framework;

import io.trino.testing.containers.environment.QueryResult;
import io.trino.tests.product.deltalake.DeltaLakeDatabricksEnvironment;
import io.trino.tests.product.deltalake.TestDatabricksWithGlueMetastoreCleanUp;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.Table;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.time.temporal.ChronoUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;

class TestDatabricksGlueMetastoreCleanup
{
    private static final Instant OLD_CREATION_TIME = Instant.now().minus(30, DAYS);

    @Test
    void testDropsDbfsDeltaTablesUsingDatabricks()
    {
        TestingEnvironment env = new TestingEnvironment("default", List.of(
                table("test_dbfs", "DELTA", "dbfs:/warehouse/test_dbfs"),
                table("test_s3", "delta", "s3://bucket/test_s3"),
                table("test_hive", "parquet", "dbfs:/warehouse/test_hive"),
                table("test_no_path", "delta", null),
                table("test_view", "delta", "dbfs:/warehouse/test_view").toBuilder().tableType("VIRTUAL_VIEW").build(),
                table("test_recent", "delta", "dbfs:/warehouse/test_recent").toBuilder().createTime(Instant.now()).build(),
                table("other", "delta", "dbfs:/warehouse/other")), 0);

        new TestDatabricksWithGlueMetastoreCleanUp().testCleanUpOldTablesUsingDelta(env);

        assertThat(env.databricksStatements).containsExactly("DROP TABLE IF EXISTS default.test_dbfs");
        assertThat(env.dropStatements()).containsExactlyInAnyOrder(
                "DROP TABLE IF EXISTS hive.default.test_s3",
                "DROP TABLE IF EXISTS hive.default.test_hive",
                "DROP TABLE IF EXISTS hive.default.test_no_path",
                "DROP VIEW IF EXISTS hive.default.test_view");
    }

    @Test
    void testDoesNotDropSchemaWhenInitialTableListingFails()
    {
        TestingEnvironment env = new TestingEnvironment("test_schema", List.of(table("test_table", "delta", "s3://bucket/test_table")), 1);

        new TestDatabricksWithGlueMetastoreCleanUp().testCleanUpOldTablesUsingDelta(env);

        assertThat(env.dropStatements()).isEmpty();
        assertThat(env.databricksStatements).isEmpty();
    }

    @Test
    void testDoesNotDropSchemaWhenFinalTableListingFails()
    {
        TestingEnvironment env = new TestingEnvironment("test_schema", List.of(table("test_table", "delta", "s3://bucket/test_table")), 2);

        new TestDatabricksWithGlueMetastoreCleanUp().testCleanUpOldTablesUsingDelta(env);

        assertThat(env.dropStatements()).containsExactly("DROP TABLE IF EXISTS hive.test_schema.test_table");
        assertThat(env.databricksStatements).isEmpty();
    }

    @Test
    void testDropsEmptySchemaAfterSuccessfulTableListing()
    {
        TestingEnvironment env = new TestingEnvironment("test_schema", List.of(), 0);

        new TestDatabricksWithGlueMetastoreCleanUp().testCleanUpOldTablesUsingDelta(env);

        assertThat(env.dropStatements()).containsExactly("DROP SCHEMA IF EXISTS hive.test_schema");
        assertThat(env.databricksStatements).isEmpty();
    }

    private static Table table(String name, String provider, String path)
    {
        return Table.builder()
                .name(name)
                .createTime(OLD_CREATION_TIME)
                .parameters(Map.of("spark.sql.sources.provider", provider))
                .storageDescriptor(storage -> storage
                        .location("s3://bucket/" + name)
                        .serdeInfo(serde -> serde.parameters(path == null ? Map.of() : Map.of("path", path))))
                .build();
    }

    private static class TestingEnvironment
            extends DeltaLakeDatabricksEnvironment
    {
        private final String schema;
        private final List<Table> tables;
        private final int failedTableListing;
        private final List<String> trinoStatements = new ArrayList<>();
        private final List<String> databricksStatements = new ArrayList<>();
        private int tableListings;

        public TestingEnvironment(String schema, List<Table> tables, int failedTableListing)
        {
            this.schema = schema;
            this.tables = tables;
            this.failedTableListing = failedTableListing;
        }

        @Override
        public QueryResult executeTrinoSql(String sql)
        {
            trinoStatements.add(sql);
            if (sql.equals("SELECT DISTINCT(table_schema) FROM hive.information_schema.tables")) {
                return QueryResult.forRows(List.of(List.of(schema)));
            }
            if (sql.startsWith("SELECT table_name FROM hive.information_schema.tables")) {
                tableListings++;
                if (tableListings == failedTableListing) {
                    throw new IllegalStateException("Table listing failed");
                }
                return QueryResult.forRows(tables.stream()
                        .map(table -> List.<Object>of(table.name()))
                        .toList());
            }
            return QueryResult.forRows(List.of());
        }

        @Override
        public QueryResult executeDatabricksSql(String sql)
        {
            databricksStatements.add(sql);
            return QueryResult.forRows(List.of());
        }

        @Override
        public GlueClient createGlueClient()
        {
            return new TestingGlueClient(tables);
        }

        public List<String> dropStatements()
        {
            return trinoStatements.stream()
                    .filter(sql -> sql.startsWith("DROP "))
                    .toList();
        }
    }

    private static class TestingGlueClient
            implements GlueClient
    {
        private final List<Table> tables;

        public TestingGlueClient(List<Table> tables)
        {
            this.tables = tables;
        }

        @Override
        public GetDatabaseResponse getDatabase(GetDatabaseRequest request)
        {
            return GetDatabaseResponse.builder()
                    .database(Database.builder().name(request.name()).createTime(OLD_CREATION_TIME).build())
                    .build();
        }

        @Override
        public GetTableResponse getTable(GetTableRequest request)
        {
            return GetTableResponse.builder()
                    .table(tables.stream().filter(table -> table.name().equals(request.name())).findFirst().orElseThrow())
                    .build();
        }

        @Override
        public String serviceName()
        {
            return SERVICE_NAME;
        }

        @Override
        public void close() {}
    }
}
