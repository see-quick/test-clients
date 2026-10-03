/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package admin.integration;

import io.strimzi.test.container.StrimziKafkaCluster;
import io.strimzi.testclients.admin.KafkaAdminClient;
import io.strimzi.testclients.configuration.ConfigurationConstants;
import io.strimzi.testclients.constants.Constants;
import io.strimzi.testclients.utils.ConfigurationUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;

/**
 * Runs the admin client against a cluster that only exposes an mTLS listener,
 * with the certificates handed over through the `configure ssl` subcommand.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AdminTlsIT {
    private static final List<String> SSL_CONFIG_KEYS = List.of(
        ConfigurationConstants.CA_CRT_PROPERTY,
        ConfigurationConstants.USER_KEY_PROPERTY,
        ConfigurationConstants.USER_CRT_PROPERTY
    );
    private static final List<String> SSL_FILE_NAMES = List.of(
        Constants.TRUSTSTORE_FILE_NAME,
        Constants.KEYSTORE_KEY_FILE_NAME,
        Constants.KEYSTORE_CERT_FILE_NAME
    );

    private static StrimziKafkaCluster kafkaCluster;
    private static Admin admin;
    private static CommandLine cmd;

    @BeforeAll
    static void setup(@TempDir Path certDir) throws Exception {
        kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withInternalTopicReplicationFactor(1)
            .withSharedNetwork()
            .withTls()
            .build();
        kafkaCluster.start();

        String caCrt = TlsUtils.caCertificate(kafkaCluster);
        String userCrt = TlsUtils.userCertificateChain(kafkaCluster);
        String userKey = TlsUtils.userKey(kafkaCluster);

        Path caCrtFile = Files.writeString(certDir.resolve("ca.crt"), caCrt);
        Path userCrtFile = Files.writeString(certDir.resolve("user.crt"), userCrt);
        Path userKeyFile = Files.writeString(certDir.resolve("user.key"), userKey);

        cmd = new CommandLine(new KafkaAdminClient());
        assertThat(cmd.execute("configure", "common", "--bootstrap-server", kafkaCluster.getBootstrapServers()), is(0));
        assertThat(cmd.execute("configure", "ssl",
            "--truststore", caCrtFile.toString(),
            "--keystore-cert", userCrtFile.toString(),
            "--keystore-key", userKeyFile.toString()), is(0));

        // Independent TLS client used only to check what the admin client did
        admin = Admin.create(Map.of(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers(),
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL",
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM",
            SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, caCrt,
            SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM",
            SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, userCrt,
            SslConfigs.SSL_KEYSTORE_KEY_CONFIG, userKey
        ));
    }

    @AfterAll
    static void afterAll() throws IOException {
        if (admin != null) {
            admin.close();
        }
        kafkaCluster.stop();
        removeSslConfiguration();
    }

    @Test
    void testTopicOperationsOverTls() throws Exception {
        String topicName = "my-tls-topic";

        cmd.execute("topic", "create", "-tp", "2", "-trf", "1", "-t", topicName);
        // Sleep for a while to prevent race condition during topics creation
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));

        TopicDescription topicDescription = admin.describeTopics(List.of(topicName)).allTopicNames().get().get(topicName);
        assertThat(topicDescription.partitions().size(), is(2));

        List<String> listedTopics = Arrays.asList(captureOutput("topic", "list").split("\\n"));
        assertThat(listedTopics, hasItem(topicName));

        cmd.execute("topic", "delete", "-t", topicName);
        // Sleep for a while to prevent race condition during topics deletion
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));

        assertThat(admin.listTopics().names().get().contains(topicName), is(false));
    }

    private static String captureOutput(String... args) {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
            cmd.execute(args);
        } finally {
            System.setOut(originalOut);
        }

        return out.toString(StandardCharsets.UTF_8);
    }

    /**
     * The admin client keeps its configuration in a shared folder (~/.admin_client by default), which the
     * plaintext ITs reuse. Leaving the SSL entries there would switch them to TLS against a plaintext cluster.
     */
    private static void removeSslConfiguration() throws IOException {
        Path configFolder = Paths.get(ConfigurationUtils.getConfigFolderPath());
        Properties properties = ConfigurationUtils.getAdminClientPropertiesIfExists();
        SSL_CONFIG_KEYS.forEach(properties::remove);

        try (FileOutputStream fileOutputStream = new FileOutputStream(configFolder.resolve("config.properties").toFile())) {
            properties.store(fileOutputStream, "Store properties to config file");
        }

        for (String fileName : SSL_FILE_NAMES) {
            Files.deleteIfExists(configFolder.resolve(fileName));
        }
    }
}
