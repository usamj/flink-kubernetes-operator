package org.apache.flink.kubernetes.operator.standalone;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.kubeclient.Fabric8FlinkStandaloneKubeClient;
import org.apache.flink.kubernetes.operator.kubeclient.FlinkStandaloneKubeClient;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.util.concurrent.Executors;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** @link KubernetesStandaloneClusterDescriptor unit tests */
@EnableKubernetesMockClient(crud = true)
public class KubernetesStandaloneClusterDescriptorTest {

    private KubernetesStandaloneClusterDescriptor clusterDescriptor;
    KubernetesMockServer mockServer;
    private NamespacedKubernetesClient kubernetesClient;
    private FlinkStandaloneKubeClient flinkKubeClient;
    private Configuration flinkConfig = new Configuration();

    @BeforeEach
    public void setup() {
        FlinkDeployment flinkDeployment = TestUtils.buildSessionCluster();
        flinkConfig = TestUtils.createTestFlinkConfig(flinkDeployment);
        kubernetesClient = mockServer.createClient().inNamespace(TestUtils.TEST_NAMESPACE);
        flinkKubeClient =
                new Fabric8FlinkStandaloneKubeClient(
                        flinkConfig, kubernetesClient, Executors.newDirectExecutorService());

        clusterDescriptor = new KubernetesStandaloneClusterDescriptor(flinkDeployment, flinkConfig, flinkKubeClient);
    }

    @Test
    public void testDeploySessionCluster() throws Exception {
        ClusterSpecification clusterSpecification = TestUtils.createClusterSpecification();

        flinkConfig.setString(BlobServerOptions.PORT, String.valueOf(0));
        flinkConfig.setString(TaskManagerOptions.RPC_PORT, String.valueOf(0));
        flinkConfig.setString(RestOptions.BIND_PORT, String.valueOf(0));

        ClusterClientProvider clusterClientProvider =
                clusterDescriptor.deploySessionCluster(clusterSpecification);

        List<Deployment> deployments =
                kubernetesClient
                        .apps()
                        .deployments()
                        .inNamespace(TestUtils.TEST_NAMESPACE)
                        .list()
                        .getItems();
        String expectedJMDeploymentName = TestUtils.CLUSTER_ID;
        String expectedTMDeploymentName = TestUtils.CLUSTER_ID + "-taskmanager";

        assertEquals(2, deployments.size());
        assertThat(
                deployments.stream()
                        .map(d -> d.getMetadata().getName())
                        .collect(Collectors.toList()),
                containsInAnyOrder(expectedJMDeploymentName, expectedTMDeploymentName));
        assertEquals(
                flinkConfig.get(BlobServerOptions.PORT),
                String.valueOf(Constants.BLOB_SERVER_PORT));
        assertEquals(
                flinkConfig.get(TaskManagerOptions.RPC_PORT),
                String.valueOf(Constants.TASK_MANAGER_RPC_PORT));
        assertEquals(flinkConfig.get(RestOptions.BIND_PORT), String.valueOf(Constants.REST_PORT));

        ClusterClient clusterClient = clusterClientProvider.getClusterClient();

        String expectedWebUrl =
                String.format(
                        "http://%s:%d",
                        ExternalServiceDecorator.getNamespacedExternalServiceName(
                                TestUtils.CLUSTER_ID, TestUtils.TEST_NAMESPACE),
                        Constants.REST_PORT);
        assertEquals(expectedWebUrl, clusterClient.getWebInterfaceURL());
    }
}
