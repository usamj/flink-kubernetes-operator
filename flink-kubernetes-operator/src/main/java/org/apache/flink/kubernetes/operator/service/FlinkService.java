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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.observer.SavepointFetchResult;
import org.apache.flink.runtime.client.JobStatusMessage;

import io.fabric8.kubernetes.api.model.PodList;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Optional;

/** The service interface for submitting and interacting with Flink clusters and jobs. */
public interface FlinkService {

    void submitApplicationCluster(FlinkDeployment deployment, Configuration conf) throws Exception;

    void submitSessionCluster(FlinkDeployment deployment, Configuration conf) throws Exception;

    void submitJobToSessionCluster(FlinkSessionJob sessionJob, Configuration conf) throws Exception;

    void stopSessionCluster(FlinkDeployment deployment, Configuration conf, boolean deleteHaData);

    void deleteCluster(FlinkDeployment deployment, Configuration conf, boolean deleteHaData);

    boolean isJobManagerPortReady(Configuration conf);

    Collection<JobStatusMessage> listJobs(Configuration conf) throws Exception;

    Optional<String> cancelJob(
            @Nullable JobID jobID,
            UpgradeMode upgradeMode,
            FlinkDeployment deployment,
            Configuration conf)
            throws Exception;

    void cancelSessionJob(JobID jobID, Configuration conf) throws Exception;

    void triggerSavepoint(FlinkDeployment deployment, Configuration conf) throws Exception;

    SavepointFetchResult fetchSavepointInfo(FlinkDeployment deployment, Configuration conf)
            throws Exception;

    PodList getJmPodList(Configuration conf);
}
