/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.timelock.config;

import java.util.List;
import java.util.stream.Collectors;

import com.palantir.timelock.utils.KubernetesHostnames;

public enum ClusterDiscoveryModes implements ClusterDiscoverer {

    /**
     * Derives members of a cluster based on the current hostname, and the expected size of the cluster.
     * <p>
     * Assumes that the server is being spun up in a stateful set.
     */
    KUBERNETES {

        public List<String> getClusterMembers(ClusterConfiguration configuration) {
            KubernetesClusterConfiguration k8sConfig = (KubernetesClusterConfiguration) configuration;
            return KubernetesHostnames.getClusterMembers(k8sConfig.expectedClusterSize())
                    .stream()
                    .map(hostname -> String.format("%s:%s", hostname, k8sConfig.nonHostnameComponent()))
                    .collect(Collectors.toList());
        }

    },

    /** Relies on cluster members being pre-populated in the configuration. */
    PREPOPULATED {
        public List<String> getClusterMembers(ClusterConfiguration configuration) {
            return configuration.cluster().uris();
        }
    }

}
