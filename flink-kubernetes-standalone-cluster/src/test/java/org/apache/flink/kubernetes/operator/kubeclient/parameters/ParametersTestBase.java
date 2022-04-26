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

package org.apache.flink.kubernetes.operator.kubeclient.parameters;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.kubeclient.utils.TestUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Base class for Kubernetes tests. */
public class ParametersTestBase {

    protected Configuration flinkConfig;

    protected ClusterSpecification clusterSpecification = TestUtils.createClusterSpecification();

    protected final Map<String, String> userLabels =
            TestUtils.generateTestStringStringMap("label", "value", 2);

    protected final Map<String, String> userAnnotations =
            TestUtils.generateTestStringStringMap("annotation", "value", 2);

    protected final Map<String, String> userNodeSelectors =
            TestUtils.generateTestStringStringMap("selector", "val", 2);

    protected final List<String> userImagePullSecrets = Arrays.asList("s1", "s2", "s3");

    protected void setupFlinkConfig() {
        flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_ANNOTATIONS, userAnnotations);
        flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_LABELS, userLabels);
        flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_LABELS, userLabels);
        flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_NODE_SELECTOR, userNodeSelectors);
        flinkConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_SECRETS, userImagePullSecrets);
    }
}
