package org.apache.flink.kubernetes.operator.kubeclient;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClient;
import org.apache.flink.kubernetes.operator.utils.StandaloneKubernetesUtils;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;

import java.util.concurrent.ExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The Implementation of {@link FlinkStandaloneKubeClient}. */
public class Fabric8FlinkStandaloneKubeClient extends Fabric8FlinkKubeClient
        implements FlinkStandaloneKubeClient {

    private final NamespacedKubernetesClient internalClient;

    public Fabric8FlinkStandaloneKubeClient(
            Configuration flinkConfig,
            NamespacedKubernetesClient client,
            ExecutorService executorService) {
        super(flinkConfig, client, executorService);
        internalClient = checkNotNull(client);
    }

    @Override
    public void createTaskManagerDeployment(Deployment tmDeployment) {
        this.internalClient.apps().deployments().create(tmDeployment);
    }

    @Override
    public void stopAndCleanupCluster(String clusterId) {
        this.internalClient
                .apps()
                .deployments()
                .withName(StandaloneKubernetesUtils.getJobManagerDeploymentName(clusterId))
                .cascading(true)
                .delete();

        this.internalClient
                .apps()
                .deployments()
                .withName(StandaloneKubernetesUtils.getTaskManagerDeploymentName(clusterId))
                .cascading(true)
                .delete();
    }
}
