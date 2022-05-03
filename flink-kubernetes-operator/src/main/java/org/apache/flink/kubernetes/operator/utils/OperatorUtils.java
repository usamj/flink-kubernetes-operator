/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.Mappers;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Operator SDK related utility functions. */
public class OperatorUtils {

    private static final String NAMESPACES_SPLITTER_KEY = ",";

    public static final String ALL_NAMESPACE_NAME = "all";

    private static final String NATIVE_JM_POSTFIX = "-native-jm";

    private static final String STANDALONE_JM_POSTFIX = "-standalone-jm";
    private static final String STANDALONE_TM_POSTFIX = "-standalone-tm";

    public static List<EventSource> createDeploymentEventSources(
            KubernetesClient kubernetesClient, String namespace) {
        List<EventSource> deploymentEventSources = new ArrayList<>();
        FilterWatchListDeletable<Deployment, DeploymentList> filteredClient =
                kubernetesClient.apps().deployments().inNamespace(namespace);

        deploymentEventSources.addAll(
                createNativeDeploymentEventSources(filteredClient, namespace));
        deploymentEventSources.addAll(
                createStandaloneDeploymentEventSources(filteredClient, namespace));
        return deploymentEventSources;
    }

    public static List<EventSource> createDeploymentEventSources(
            KubernetesClient kubernetesClient) {
        FilterWatchListDeletable<Deployment, DeploymentList> filteredClient =
                kubernetesClient.apps().deployments().inAnyNamespace();

        List<EventSource> deploymentEventSources = new ArrayList<>();
        deploymentEventSources.addAll(
                createStandaloneDeploymentEventSources(filteredClient, ALL_NAMESPACE_NAME));
        deploymentEventSources.addAll(
                createNativeDeploymentEventSources(filteredClient, ALL_NAMESPACE_NAME));

        return deploymentEventSources;
    }

    private static List<EventSource> createNativeDeploymentEventSources(
            FilterWatchListDeletable<Deployment, DeploymentList> filteredClient, String namespace) {
        final Map<String, String> labels = new HashMap<>();
        labels.put(Constants.LABEL_TYPE_KEY, Constants.LABEL_TYPE_NATIVE_TYPE);
        labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
        return List.of(
                createDeploymentInformerEventSource(
                        filteredClient, getNativeJmDeploymentIdentifier(namespace), labels));
    }

    public static List<EventSource> createStandaloneDeploymentEventSources(
            FilterWatchListDeletable<Deployment, DeploymentList> filteredClient, String namespace) {
        final Map<String, String> commonLabels = new HashMap<>();
        commonLabels.put(
                Constants.LABEL_TYPE_KEY, StandaloneKubernetesUtils.LABEL_TYPE_STANDALONE_TYPE);

        final Map<String, String> jmLabels = new HashMap<>(commonLabels);
        jmLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);

        return List.of(
                createDeploymentInformerEventSource(
                        filteredClient, getStandaloneJmDeploymentIdentifier(namespace), jmLabels));
    }

    private static InformerEventSource<Deployment, HasMetadata> createDeploymentInformerEventSource(
            FilterWatchListDeletable<Deployment, DeploymentList> filteredClient,
            String name,
            Map<String, String> labels) {
        SharedIndexInformer<Deployment> informer =
                filteredClient.withLabels(labels).runnableInformer(0);

        return new InformerEventSource<>(informer, Mappers.fromLabel(Constants.LABEL_APP_KEY)) {
            @Override
            public String name() {
                return name;
            }
        };
    }

    public static Set<String> getWatchedNamespaces() {
        String watchedNamespaces = EnvUtils.get(EnvUtils.ENV_WATCHED_NAMESPACES);

        if (StringUtils.isEmpty(watchedNamespaces)) {
            return Collections.emptySet();
        } else {
            return new HashSet<>(Arrays.asList(watchedNamespaces.split(NAMESPACES_SPLITTER_KEY)));
        }
    }

    public static Optional<FlinkDeployment> getSecondaryResource(
            FlinkSessionJob sessionJob,
            Context context,
            FlinkOperatorConfiguration operatorConfiguration) {
        var identifier =
                operatorConfiguration.getWatchedNamespaces().size() >= 1
                        ? sessionJob.getMetadata().getNamespace()
                        : null;
        return context.getSecondaryResource(FlinkDeployment.class, identifier);
    }

    public static String getStandaloneJmDeploymentIdentifier(String namespace) {
        return namespace + STANDALONE_JM_POSTFIX;
    }

    public static String getNativeJmDeploymentIdentifier(String namespace) {
        return namespace + NATIVE_JM_POSTFIX;
    }
}
