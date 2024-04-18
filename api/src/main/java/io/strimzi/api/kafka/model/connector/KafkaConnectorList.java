/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connector;

import io.fabric8.kubernetes.api.model.DefaultKubernetesResourceList;
import lombok.ToString;

/**
 * A {@code DefaultKubernetesResourceList<KafkaConnector>} required for using Fabric8 CRD support.
 */
@ToString(callSuper = true)
public class KafkaConnectorList extends DefaultKubernetesResourceList<KafkaConnector> { }
