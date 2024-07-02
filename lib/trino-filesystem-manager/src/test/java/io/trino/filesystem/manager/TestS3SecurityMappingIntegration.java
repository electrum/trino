package io.trino.filesystem.manager;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemModule;
import io.trino.spi.NodeManager;
import io.trino.spi.security.ConnectorIdentity;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.io.Resources.getResource;
import static com.google.common.reflect.Reflection.newProxy;

public class TestS3SecurityMappingIntegration
{
    @Test
    void testIntegration()
            throws Exception
    {
        try (MockWebServer server = new MockWebServer()) {
            NodeManager nodeManager = newProxy(NodeManager.class, (_, _, _) -> {
                throw new UnsupportedOperationException();
            });

            Map<String, String> config = ImmutableMap.<String, String>builder()
                    .put("fs.hadoop.enabled", "false")
                    .put("fs.native-s3.enabled", "true")
                    .put("s3.aws-access-key", "testAccessKey")
                    .put("s3.aws-secret-key", "testSecretKey")
                    .put("s3.endpoint", server.url("/").toString())
                    .put("s3.region", "us-east-1")
                    .put("s3.path-style-access", "true")
                    .put("s3.sts.endpoint", server.url("/").toString())
                    .put("s3.security-mapping.enabled", "true")
                    .put("s3.security-mapping.config-file", getResource(getClass(), "security-mapping.json").getPath())
                    .buildOrThrow();

            Bootstrap app = new Bootstrap(
                    binder -> binder.bind(OpenTelemetry.class).toInstance(OpenTelemetry.noop()),
                    binder -> binder.bind(Tracer.class).toInstance(OpenTelemetry.noop().getTracer("test")),
                    new FileSystemModule("test", nodeManager, OpenTelemetry.noop()),
                    new S3FileSystemModule());

            Injector injector = app
                    .setRequiredConfigurationProperties(config)
                    .doNotInitializeLogging()
                    .quiet()
                    .initialize();

            TrinoFileSystemFactory fileSystemFactory = injector.getInstance(TrinoFileSystemFactory.class);
            TrinoFileSystem fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));

            server.enqueue(new MockResponse().setBody(
                    """
                    <AssumeRoleResponse>
                      <AssumeRoleResult>
                      <SourceIdentity>Alice</SourceIdentity>
                        <AssumedRoleUser>
                          <Arn>arn:aws:sts::123456789012:assumed-role/demo/TestAR</Arn>
                          <AssumedRoleId>ARO123EXAMPLE123:TestAR</AssumedRoleId>
                        </AssumedRoleUser>
                        <Credentials>
                          <AccessKeyId>ASIAIOSFODNN7EXAMPLE</AccessKeyId>
                          <SecretAccessKey>wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY</SecretAccessKey>
                          <SessionToken>test</SessionToken>
                          <Expiration>2019-11-09T13:34:41Z</Expiration>
                        </Credentials>
                      </AssumeRoleResult>
                    </AssumeRoleResponse>"""));

            for (int i = 0; i < 10; i++) {
                server.enqueue(new MockResponse());
            }

            fileSystem.listFiles(Location.of("s3://test/"));

            injector.getInstance(LifeCycleManager.class).stop();

            System.out.println("request count: " + server.getRequestCount());
            for (int i = 0; i < server.getRequestCount(); i++) {
                RecordedRequest request = server.takeRequest();
                System.out.println("headers: " + request.getHeaders().toMultimap());
                System.out.println("body: " + request.getBody().readUtf8());
            }
        }
    }
}
