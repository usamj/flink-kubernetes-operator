/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.service;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.client.cli.ApplicationDeployer;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.deployment.application.cli.ApplicationClusterDeployer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.StandaloneKubernetesUtils;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;

/**
 * Implementation of {@link FlinkService} submitting and interacting with Native Kubernetes Flink
 * clusters and jobs.
 */
public class FlinkNativeService extends AbstractFlinkService {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkNativeService.class);


    public FlinkNativeService(
            NamespacedKubernetesClient kubernetesClient,
            FlinkOperatorConfiguration operatorConfiguration) {
        super(kubernetesClient, operatorConfiguration);
    }

    @Override
    public void submitApplicationCluster(FlinkDeployment deployment, Configuration conf)
            throws Exception {
        if (FlinkUtils.isKubernetesHAActivated(conf)) {
            final String clusterId =
                    Preconditions.checkNotNull(conf.get(KubernetesConfigOptions.CLUSTER_ID));
            final String namespace =
                    Preconditions.checkNotNull(conf.get(KubernetesConfigOptions.NAMESPACE));
            // Delete the job graph in the HA ConfigMaps so that the newly changed job config(e.g.
            // parallelism) could take effect
            deleteJobGraphInKubernetesHA(clusterId, namespace, kubernetesClient);
        }
        LOG.info("Deploying application cluster");
        final ClusterClientServiceLoader clusterClientServiceLoader =
                new DefaultClusterClientServiceLoader();
        final ApplicationDeployer deployer =
                new ApplicationClusterDeployer(clusterClientServiceLoader);

        JobSpec jobSpec = deployment.getSpec().getJob();
        final ApplicationConfiguration applicationConfiguration =
                new ApplicationConfiguration(
                        jobSpec.getArgs() != null ? jobSpec.getArgs() : new String[0],
                        jobSpec. getEntryClass());

        deployer.run(conf, applicationConfiguration);
        LOG.info("Application cluster successfully deployed");
    }

    @Override
    public void submitSessionCluster(FlinkDeployment deployment, Configuration conf)
            throws Exception {
        LOG.info("Deploying session cluster");
        final ClusterClientServiceLoader clusterClientServiceLoader =
                new DefaultClusterClientServiceLoader();
        final ClusterClientFactory<String> kubernetesClusterClientFactory =
                clusterClientServiceLoader.getClusterClientFactory(conf);
        try (final ClusterDescriptor<String> kubernetesClusterDescriptor =
                kubernetesClusterClientFactory.createClusterDescriptor(conf)) {
            kubernetesClusterDescriptor.deploySessionCluster(
                    kubernetesClusterClientFactory.getClusterSpecification(conf));
        }
        LOG.info("Session cluster successfully deployed");
    }

    @Override
    public void stopSessionCluster(
            FlinkDeployment deployment, Configuration conf, boolean deleteHaData) {
        stopCluster(deployment, deleteHaData);
    }

    @Override
    public void deleteCluster(
            FlinkDeployment deployment, Configuration conf, boolean deleteHaData) {
        stopCluster(deployment, deleteHaData);
    }

    private void stopCluster(FlinkDeployment deployment, boolean deleteHaData) {
        deleteClusterInternal(deployment, deleteHaData);
        waitForClusterShutdown(deployment);
    }

    private void deleteClusterInternal(FlinkDeployment deployment, boolean deleteHaConfigmaps) {
        final String clusterId = deployment.getMetadata().getName();
        final String namespace = deployment.getMetadata().getNamespace();

        LOG.info("Deleting Flink cluster resources");
        kubernetesClient
                .apps()
                .deployments()
                .inNamespace(namespace)
                .withName(KubernetesUtils.getDeploymentName(clusterId))
                .cascading(true)
                .delete();

        if (deleteHaConfigmaps) {
            // We need to wait for cluster shutdown otherwise HA configmaps might be recreated
            waitForClusterShutdown(namespace, clusterId);
            kubernetesClient
                    .configMaps()
                    .inNamespace(namespace)
                    .withLabels(
                            StandaloneKubernetesUtils.getConfigMapLabels(
                                    clusterId, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY))
                    .delete();
        }
    }

    @Override
    protected PodList getJmPodList(String namespace, String clusterId) {
        return kubernetesClient
                .pods()
                .inNamespace(namespace)
                .withLabels(KubernetesUtils.getJobManagerSelectors(clusterId))
                .list();
    }

    @VisibleForTesting
    protected void deleteJobGraphInKubernetesHA(
            String clusterId, String namespace, KubernetesClient kubernetesClient) {
        // The HA ConfigMap names have been changed from 1.15, so we use the labels to filter out
        // them and delete job graph key
        final Map<String, String> haConfigMapLabels =
                KubernetesUtils.getConfigMapLabels(
                        clusterId, Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY);
        final ConfigMapList configMaps =
                kubernetesClient
                        .configMaps()
                        .inNamespace(namespace)
                        .withLabels(haConfigMapLabels)
                        .list();

        configMaps
                .getItems()
                .forEach(
                        configMap -> {
                            final boolean isDeleted =
                                    configMap
                                            .getData()
                                            .entrySet()
                                            .removeIf(FlinkNativeService::isJobGraphKey);
                            if (isDeleted) {
                                LOG.info(
                                        "Job graph in ConfigMap {} is deleted",
                                        configMap.getMetadata().getName());
                            }
                        });
        kubernetesClient.resourceList(configMaps).inNamespace(namespace).createOrReplace();
    }

    private static boolean isJobGraphKey(Map.Entry<String, String> entry) {
        return entry.getKey().startsWith(Constants.JOB_GRAPH_STORE_KEY_PREFIX);
    }
}
