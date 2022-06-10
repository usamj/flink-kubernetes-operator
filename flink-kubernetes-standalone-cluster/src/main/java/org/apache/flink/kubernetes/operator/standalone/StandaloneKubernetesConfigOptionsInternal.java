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

package org.apache.flink.kubernetes.operator.standalone;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds internal configuration constants used by flink operator when deploying flink
 * clusters in standalone mode.
 */
public class StandaloneKubernetesConfigOptionsInternal {
    public static final ConfigOption<Integer> KUBERNETES_TASKMANAGER_REPLICAS =
            key("kubernetes.internal.taskmanager.replicas")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Specify how many pods will be in the TaskManager pool. For "
                                    + "standalone kubernetes Flink sessions clusters.");

    public static final ConfigOption<Boolean> APPLICATION_CLUSTER =
            key("kubernetes.internal.application.cluster")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Specify whether the cluster will be deployed in application mode");

    public static final ConfigOption<String> APPLICATION_JAR_MAIN_CLASS =
            key("kubernetes.internal.application.main-class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Fully qualified main class name of the Flink job.");
}
