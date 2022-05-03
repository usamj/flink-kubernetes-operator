package org.apache.flink.kubernetes.operator.crd.spec;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Enum to control Flink deployment mode on Kubernetes. */
@Experimental
public enum KubernetesDeploymentMode {

    /**
     * Deploys Flink using Flinks native Kubernetes support. Only supported for newer versions of
     * Flink
     */
    @JsonProperty("native")
    NATIVE,

    /** Deploys Flink on-top of kubernetes in standalone mode. */
    @JsonProperty("standalone")
    STANDALONE;

    public static KubernetesDeploymentMode getDeploymentMode(FlinkDeployment flinkDeployment) {
        return getDeploymentMode(flinkDeployment.getSpec());
    }

    public static KubernetesDeploymentMode getDeploymentMode(FlinkDeploymentSpec spec) {
        return spec.getMode() == null ? KubernetesDeploymentMode.NATIVE : spec.getMode();
    }
}
