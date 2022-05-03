package org.apache.flink.kubernetes.operator.service;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.KubernetesClusterClientFactory;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.artifact.JarResolver;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.kubeclient.Fabric8FlinkStandaloneKubeClient;
import org.apache.flink.kubernetes.operator.kubeclient.FlinkStandaloneKubeClient;
import org.apache.flink.kubernetes.operator.standalone.KubernetesStandaloneClusterDescriptor;
import org.apache.flink.kubernetes.operator.utils.StandaloneKubernetesUtils;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;

/**
 * Implementation of {@link FlinkService} submitting and interacting with Standalone Kubernetes
 * Flink clusters and jobs.
 */
public class FlinkStandaloneService extends AbstractFlinkService {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkStandaloneService.class);

    private final JarResolver jarResolver;
    private final ExecutorService executorService;
    FlinkStandaloneKubeClient kubeClient;

    public FlinkStandaloneService(
            NamespacedKubernetesClient kubernetesClient,
            FlinkOperatorConfiguration operatorConfiguration) {
        super(kubernetesClient, operatorConfiguration);
        this.jarResolver = new JarResolver();
        this.executorService =
                Executors.newFixedThreadPool(
                        4, new ExecutorThreadFactory("Flink-RestClusterClient-IO"));
    }

    @Override
    public void submitApplicationCluster(FlinkDeployment deployment, Configuration conf)
            throws Exception {
        LOG.info("Deploying application cluster");
        submitClusterInternal(deployment, conf);
        LOG.info("Application cluster successfully deployed");
    }

    @Override
    public void submitSessionCluster(FlinkDeployment deployment, Configuration conf)
            throws Exception {
        LOG.info("Deploying session cluster");
        submitClusterInternal(deployment, conf);
        LOG.info("Session cluster successfully deployed");
    }

    @Override
    public void stopSessionCluster(
            FlinkDeployment deployment, Configuration conf, boolean deleteHaData) {
        deleteCluster(deployment, conf, deleteHaData);
        waitForClusterShutdown(deployment);
    }

    private ClusterSpecification getClusterSpecification(Configuration conf) {
        return new KubernetesClusterClientFactory().getClusterSpecification(conf);
    }

    @Override
    protected PodList getJmPodList(String namespace, String clusterId) {
        return kubernetesClient
                .pods()
                .inNamespace(namespace)
                .withLabels(StandaloneKubernetesUtils.getJobManagerSelectors(clusterId))
                .list();
    }

    @Override
    public void deleteCluster(
            FlinkDeployment deployment, Configuration conf, boolean deleteHaData) {
        stopCluster(deployment, deleteHaData);
    }

    @VisibleForTesting
    protected FlinkStandaloneKubeClient createNamespacedKubeClient(Configuration configuration, String namespace) {
        NamespacedKubernetesClient client = new DefaultKubernetesClient().inNamespace(namespace);

        final int poolSize =
                configuration.get(KubernetesConfigOptions.KUBERNETES_CLIENT_IO_EXECUTOR_POOL_SIZE);

        ExecutorService executorService = Executors.newFixedThreadPool(
                poolSize, new ExecutorThreadFactory("flink-kubeclient-io-for-standalone-service"));

        return new Fabric8FlinkStandaloneKubeClient(
                configuration, client, executorService);
    }

    private void submitClusterInternal(FlinkDeployment deployment, Configuration conf)
            throws ClusterDeploymentException {
        final String namespace = deployment.getMetadata().getNamespace();

        FlinkStandaloneKubeClient client = createNamespacedKubeClient(conf, namespace);
        try (final KubernetesStandaloneClusterDescriptor kubernetesClusterDescriptor =
                     new KubernetesStandaloneClusterDescriptor(deployment, conf, client)) {
            kubernetesClusterDescriptor.deploySessionCluster(getClusterSpecification(conf));
        }
    }

    private void stopCluster(FlinkDeployment deployment, boolean deleteHaData) {
        deleteClusterInternal(deployment, deleteHaData);
        waitForClusterShutdown(deployment);
    }

    private void deleteClusterInternal(FlinkDeployment deployment, boolean deleteHaConfigmaps) {
        final String clusterId = deployment.getMetadata().getName();
        final String namespace = deployment.getMetadata().getNamespace();

        LOG.info("Deleting Flink Standalone cluster TM resources");
        kubernetesClient
                .apps()
                .deployments()
                .inNamespace(namespace)
                .withName(StandaloneKubernetesUtils.getTaskManagerDeploymentName(clusterId))
                .cascading(true)
                .delete();

        LOG.info("Deleting Flink Standalone cluster JM resources");
        kubernetesClient
                .apps()
                .deployments()
                .inNamespace(namespace)
                .withName(StandaloneKubernetesUtils.getJobManagerDeploymentName(clusterId))
                .cascading(true)
                .delete();

        if (deleteHaConfigmaps) {
            // We need to wait for cluster shutdown otherwise HA configmaps might be recreated
            waitForClusterShutdown(namespace, clusterId);
            kubernetesClient
                    .configMaps()
                    .inNamespace(namespace)
                    .withLabels(
                            KubernetesUtils.getConfigMapLabels(
                                    clusterId, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY))
                    .delete();
        }
    }
}
