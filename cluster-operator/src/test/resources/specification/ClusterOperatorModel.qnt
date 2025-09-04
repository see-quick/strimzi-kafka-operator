// -*- mode: Bluespec; -*-

module ClusterOperatorModel {
  // ============================================================================
  // TYPE ALIASES
  // ============================================================================
  
  type NodeId = int
  type PodName = str
  type ClusterId = str
  type MetadataVersion = int
  type CertificateSerial = str
  
  type PodState = 
    | PodNotReady
    | PodReady  
    | PodTerminating
    | PodFailed
  
  type CertificateState = {
    serial: CertificateSerial,
    validFrom: int,
    validUntil: int,
    isActive: bool
  }
  
  type KafkaPod = {
    name: PodName,
    nodeId: NodeId,
    state: PodState,
    clusterId: ClusterId,
    metadataVersion: MetadataVersion,
    isLeader: bool,
    hasQuorum: bool
  }
  
  type NodePool = {
    name: str,
    replicas: int,
    currentPods: Set[PodName],
    targetPods: Set[PodName]
  }
  
  type ClusterState = {
    clusterId: ClusterId,
    pods: PodName -> KafkaPod,
    nodePools: str -> NodePool,
    clusterCa: CertificateState,
    clientsCa: CertificateState,
    minReadyReplicas: int,
    isRollingUpdate: bool,
    autoRebalanceEnabled: bool,
    scaledUpBrokers: Set[NodeId],
    processedEvents: int
  }
  
  // ============================================================================
  // PARAMETERS AND CONSTANTS
  // ============================================================================
  
  pure val POTENTIAL_PODS = Set("kafka-0", "kafka-1", "kafka-2", "kafka-3", "kafka-4")
  pure val POTENTIAL_NODE_POOLS = Set("pool-1", "pool-2")
  pure val MAX_METADATA_VERSION = 5
  pure val MIN_CLUSTER_SIZE = 1
  pure val MAX_CLUSTER_SIZE = 5
  pure val CERT_VALIDITY_PERIOD = 100
  
  // Parameters for bounded model checking
  pure val parameters = {
    maxProcessedEvents: 10,
    potentialPods: POTENTIAL_PODS,
    potentialNodePools: POTENTIAL_NODE_POOLS,
    enableRollingUpdates: true,
    enableCertRotation: true,
    enableAutoRebalance: true
  }
  
  // ============================================================================
  // STATE VARIABLES
  // ============================================================================
  
  var globalState: ClusterState
  
  // ============================================================================
  // ACTIONS
  // ============================================================================
  
  action init: bool = all {
    globalState' = {
      clusterId: "test-cluster-1",
      pods: Map(),
      nodePools: Map("pool-1" -> { name: "pool-1", replicas: 3, currentPods: Set(), targetPods: Set() }),
      clusterCa: { serial: "ca-1", validFrom: 0, validUntil: CERT_VALIDITY_PERIOD, isActive: true },
      clientsCa: { serial: "clients-ca-1", validFrom: 0, validUntil: CERT_VALIDITY_PERIOD, isActive: true },
      minReadyReplicas: 2,
      isRollingUpdate: false,
      autoRebalanceEnabled: true,
      scaledUpBrokers: Set(),
      processedEvents: 0
    }
  }
  
  // Create a new Kafka pod
  action createPod(podName: str, nodeId: int, poolName: str): bool = all {
    // Preconditions
    parameters.potentialPods.contains(podName),
    parameters.potentialNodePools.contains(poolName),
    not(globalState.pods.keys().contains(podName)),
    globalState.nodePools.keys().contains(poolName),
    nodeId >= 0,
    nodeId < 1000,
    
    // Effects
    val newPod = {
      name: podName,
      nodeId: nodeId,
      state: PodNotReady,
      clusterId: globalState.clusterId,
      metadataVersion: 0,
      isLeader: false,
      hasQuorum: false
    }
    
    globalState' = {
      ...globalState,
      pods: globalState.pods.put(podName, newPod),
      nodePools: globalState.nodePools.setBy(poolName, pool => 
        { ...pool, currentPods: pool.currentPods.union(Set(podName)) }
      ),
      processedEvents: globalState.processedEvents + 1
    }
  }
  
  // Pod becomes ready
  action podReady(podName: str) = all {
    // Preconditions
    globalState.pods.keys().contains(podName),
    globalState.pods.get(podName).state == PodNotReady,
    
    // Effects
    val updatedPod = { ...globalState.pods.get(podName), state: PodReady }
    globalState' = {
      ...globalState,
      pods: globalState.pods.put(podName, updatedPod),
      processedEvents: globalState.processedEvents + 1
    }
  }
  
  // Start rolling update
  action startRollingUpdate = all {
    // Preconditions
    not(globalState.isRollingUpdate),
    globalState.pods.keys().size() > 0,
    
    // Effects
    globalState' = {
      ...globalState,
      isRollingUpdate: true,
      processedEvents: globalState.processedEvents + 1
    }
  }
  
  // Rolling update a specific pod
  action rollingUpdatePod(podName: str) = all {
    // Preconditions
    globalState.isRollingUpdate,
    globalState.pods.keys().contains(podName),
    globalState.pods.get(podName).state == PodReady,
    // Maintain minimum ready replicas during rolling update
    val readyPods = globalState.pods.values().filter(pod => pod.state == PodReady)
    readyPods.size() > globalState.minReadyReplicas,
    
    // Effects
    val updatedPod = { ...globalState.pods.get(podName), state: PodTerminating }
    globalState' = {
      ...globalState,
      pods: globalState.pods.put(podName, updatedPod),
      processedEvents: globalState.processedEvents + 1
    }
  }
  
  // Complete rolling update
  action completeRollingUpdate = all {
    globalState.isRollingUpdate,
    // All pods should be ready again
    globalState.pods.values().forall(pod => pod.state == PodReady),
    
    globalState' = {
      ...globalState,
      isRollingUpdate: false,
      processedEvents: globalState.processedEvents + 1
    }
  }
  
  // Rotate cluster CA certificate
  action rotateCertificate(certType: str) = all {
    // Preconditions
    parameters.enableCertRotation,
    Set("cluster", "clients").contains(certType),
    
    // Effects
    val newSerial = if (certType == "cluster") "ca-2" else "clients-ca-2"
    val currentTime = globalState.processedEvents
    val newCert = {
      serial: newSerial,
      validFrom: currentTime,
      validUntil: currentTime + CERT_VALIDITY_PERIOD,
      isActive: true
    }
    
    globalState' = if (certType == "cluster") {
      { ...globalState, 
        clusterCa: newCert,
        processedEvents: globalState.processedEvents + 1 }
    } else {
      { ...globalState,
        clientsCa: newCert,
        processedEvents: globalState.processedEvents + 1 }
    }
  }
  
  // Scale node pool
  action scaleNodePool(poolName: str, newReplicas: int) = all {
    // Preconditions
    globalState.nodePools.keys().contains(poolName),
    newReplicas >= MIN_CLUSTER_SIZE,
    newReplicas <= MAX_CLUSTER_SIZE,
    newReplicas != globalState.nodePools.get(poolName).replicas,
    
    // Effects
    val pool = globalState.nodePools.get(poolName)
    val isScaleUp = newReplicas > pool.replicas
    val newScaledUpBrokers = if (isScaleUp) {
      // Add new broker node IDs to scaled up set
      val newNodeIds = range(pool.replicas, newReplicas - 1).map(i => 100 + i)
      globalState.scaledUpBrokers.union(newNodeIds)
    } else globalState.scaledUpBrokers
    
    globalState' = {
      ...globalState,
      nodePools: globalState.nodePools.setBy(poolName, p => { ...p, replicas: newReplicas }),
      scaledUpBrokers: newScaledUpBrokers,
      processedEvents: globalState.processedEvents + 1
    }
  }
  
  // Update metadata version
  action updateMetadataVersion(newVersion: int) = all {
    // Preconditions
    newVersion >= 0,
    newVersion <= MAX_METADATA_VERSION,
    // Metadata version should only increase
    val currentMaxVersion = if (globalState.pods.keys().size() > 0) {
      globalState.pods.values().map(pod => pod.metadataVersion).max()
    } else 0
    newVersion >= currentMaxVersion,
    
    // Effects - update all ready pods
    val updatedPods = globalState.pods.mapBy(podName => pod => 
      if (pod.state == PodReady) { ...pod, metadataVersion: newVersion } else pod
    )
    
    globalState' = {
      ...globalState,
      pods: updatedPods,
      processedEvents: globalState.processedEvents + 1
    }
  }
  
  // Process next event (advance time)
  action processNextEvent = {
    globalState' = {
      ...globalState,
      processedEvents: globalState.processedEvents + 1
    }
  }
  
  // ============================================================================
  // INVARIANTS
  // ============================================================================
  
  // Cluster ID consistency
  val ClusterIdConsistency = {
    globalState.pods.values().forall(pod => 
      pod.clusterId == globalState.clusterId
    )
  }
  
  // Metadata version monotonicity  
  val MetadataVersionMonotonicity = {
    val readyPods = globalState.pods.values().filter(pod => pod.state == PodReady)
    readyPods.size() <= 1 or {
      val versions = readyPods.map(pod => pod.metadataVersion)
      val minVersion = versions.min()
      val maxVersion = versions.max()
      maxVersion - minVersion <= 1  // Allow at most 1 version difference during rolling upgrades
    }
  }
  
  // Minimum ready replicas during operations
  val MinimumReadyReplicas = {
    val readyPods = globalState.pods.values().filter(pod => pod.state == PodReady)
    not(globalState.isRollingUpdate) or readyPods.size() >= globalState.minReadyReplicas
  }
  
  // Certificate validity overlap during rotation
  val CertificateValidityOverlap = {
    val currentTime = globalState.processedEvents
    globalState.clusterCa.validUntil > currentTime and
    globalState.clientsCa.validUntil > currentTime
  }
  
  // Node ID uniqueness
  val NodeIdUniqueness = {
    val readyPods = globalState.pods.values().filter(pod => pod.state == PodReady)
    val nodeIds = readyPods.map(pod => pod.nodeId)
    nodeIds.size() == nodeIds.toSet().size()
  }
  
  // Auto-rebalance state consistency
  val AutoRebalanceConsistency = {
    val readyPods = globalState.pods.values().filter(pod => pod.state == PodReady)
    val readyNodeIds = readyPods.map(pod => pod.nodeId)
    globalState.scaledUpBrokers.subseteq(readyNodeIds)
  }
  
  // All safety invariants together
  val AllInvariantsHold = and {
    ClusterIdConsistency,
    MetadataVersionMonotonicity,
    MinimumReadyReplicas,
    CertificateValidityOverlap,
    NodeIdUniqueness,
    AutoRebalanceConsistency
  }
  
  // ============================================================================
  // TEMPORAL PROPERTIES
  // ============================================================================
  
  // Eventually all pods become ready after creation
  temporal EventualReadiness = always {
    globalState.pods.keys().size() > 0 implies eventually {
      globalState.pods.values().forall(pod => 
        pod.state == PodReady or pod.state == PodTerminating
      )
    }
  }
  
  // Rolling updates eventually complete
  temporal RollingUpdateCompletion = always {
    globalState.isRollingUpdate implies eventually not(globalState.isRollingUpdate)
  }
  
  // ============================================================================
  // TRANSITIONS
  // ============================================================================
  
  action step = any {
    // Pod lifecycle actions
    nondet podName = oneOf(parameters.potentialPods)
    nondet nodeId = oneOf(range(0, 10))
    nondet poolName = oneOf(parameters.potentialNodePools) {
      createPod(podName, nodeId, poolName)
    },
    
    nondet podName = oneOf(globalState.pods.keys()) {
      podReady(podName)
    },
    
    // Rolling update actions
    all {
      parameters.enableRollingUpdates,
      startRollingUpdate
    },
    
    nondet podName = oneOf(globalState.pods.keys()) {
      all {
        parameters.enableRollingUpdates,
        rollingUpdatePod(podName)
      }
    },
    
    all {
      parameters.enableRollingUpdates,
      completeRollingUpdate
    },
    
    // Certificate rotation
    nondet certType = oneOf(Set("cluster", "clients")) {
      all {
        parameters.enableCertRotation,
        rotateCertificate(certType)
      }
    },
    
    // Scaling operations
    nondet poolName = oneOf(parameters.potentialNodePools)
    nondet newReplicas = oneOf(range(MIN_CLUSTER_SIZE, MAX_CLUSTER_SIZE)) {
      scaleNodePool(poolName, newReplicas)
    },
    
    // Metadata operations
    nondet newVersion = oneOf(range(0, MAX_METADATA_VERSION)) {
      updateMetadataVersion(newVersion)
    },
    
    // Time advancement
    processNextEvent
  }
  
  // Main transition system
  action next = {
    if (globalState.processedEvents < parameters.maxProcessedEvents) {
      step
    } else {
      all {
        // Stop when reaching max events
        globalState' = globalState
      }
    }
  }
}