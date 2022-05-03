package org.apache.flink.kubernetes.operator.kubeclient;

import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;

import io.fabric8.kubernetes.api.model.apps.Deployment;

/** Extension of the FlinkKubeClient that is used for Flink standalone deployments. */
public interface FlinkStandaloneKubeClient extends FlinkKubeClient {
    void createTaskManagerDeployment(Deployment tmDeployment);
}
