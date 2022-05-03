package org.apache.flink.kubernetes.operator.service;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.artifact.JarResolver;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.kubeclient.Fabric8FlinkStandaloneKubeClient;
import org.apache.flink.kubernetes.operator.kubeclient.FlinkStandaloneKubeClient;
import org.apache.flink.kubernetes.operator.utils.FlinkConfigBuilder;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;

/** @link FlinkStandaloneService unit tests */
@EnableKubernetesMockClient(crud = true)
public class FlinkStandaloneServiceTest {
    KubernetesMockServer mockServer;

    private NamespacedKubernetesClient kubernetesClient;
    FlinkStandaloneService flinkStandaloneService;
    Configuration configuration = new Configuration();

    @BeforeEach
    public void setup() {
        configuration = TestUtils.createTestFlinkConfig();

        kubernetesClient = mockServer.createClient().inAnyNamespace();
        flinkStandaloneService =
                Mockito.spy(
                        new FlinkStandaloneService(
                                kubernetesClient,
                                FlinkOperatorConfiguration.fromConfiguration(configuration)));

        ExecutorService executorService = Executors.newFixedThreadPool(
                1, new ExecutorThreadFactory("flink-kubeclient-io-for-standalone-service"));

        FlinkStandaloneKubeClient kubeClient = new Fabric8FlinkStandaloneKubeClient(
                configuration, kubernetesClient.inNamespace(TestUtils.TEST_NAMESPACE), executorService);

        doReturn(kubeClient).when(flinkStandaloneService).createNamespacedKubeClient(any(), anyString());
    }

    @Test
    public void testSubmitSessionCluster() throws Exception {
        FlinkDeployment flinkDeployment = TestUtils.buildSessionCluster();
        flinkDeployment.getSpec().setMode(KubernetesDeploymentMode.STANDALONE);

        configuration = FlinkConfigBuilder.buildFrom(flinkDeployment, configuration);
        flinkStandaloneService.submitSessionCluster(flinkDeployment, configuration);

        List<Deployment> deployments = kubernetesClient.apps().deployments().list().getItems();
        List<Service> services = kubernetesClient.services().list().getItems();

        assertEquals(2, deployments.size());
        assertEquals(2, services.size());
    }

    @Test
    public void testSubmitApplicationCluster() throws Exception {
        FlinkDeployment flinkDeployment = TestUtils.buildApplicationCluster();
        flinkDeployment.getSpec().setMode(KubernetesDeploymentMode.STANDALONE);

        configuration = FlinkConfigBuilder.buildFrom(flinkDeployment, configuration);
        flinkStandaloneService.submitApplicationCluster(flinkDeployment, configuration);

        List<Deployment> deployments = kubernetesClient.apps().deployments().list().getItems();
        List<Service> services = kubernetesClient.services().list().getItems();

        assertEquals(2, deployments.size());
        assertEquals(2, services.size());
    }

    @Test
    public void testStopSessionCluster() throws Exception {
        FlinkDeployment flinkDeployment = TestUtils.buildSessionCluster();
        flinkDeployment.getSpec().setMode(KubernetesDeploymentMode.STANDALONE);

        configuration = FlinkConfigBuilder.buildFrom(flinkDeployment, configuration);

        // We need to mock this as we wait for JM services to get deleted but in the mock CRUD
        // server this won't happen
        // as we rely on k8s controller to delete it based on ownerReference
        doNothing().when(flinkStandaloneService).waitForClusterShutdown(anyString(), anyString());

        flinkStandaloneService.submitSessionCluster(flinkDeployment, configuration);

        List<Deployment> deployments = kubernetesClient.apps().deployments().list().getItems();

        assertEquals(2, deployments.size());

        flinkStandaloneService.stopSessionCluster(flinkDeployment, configuration, false);

        deployments = kubernetesClient.apps().deployments().list().getItems();

        assertEquals(0, deployments.size());

        Path path = new JarResolver().resolve("file:///opt/flink/examples/streaming/StateMachineExample.jar");
        Path q = path.getParent();
        String p = path.toString();
    }
}
