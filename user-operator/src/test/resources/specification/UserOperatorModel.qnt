// -*- mode: Bluespec; -*-

module UserOperatorModel {
  // ============================================================================
  // TYPE ALIASES
  // ============================================================================
  // Metadata is abstracted
  type KafkaUserMetadata = {
    name: str
    // labels: str -> str  (abstracted out)
    // annotations: str -> str (abstracted out)
  }

  type KafkaUserAuthentication =
    | ScramSha512Authentication({ secretName: str, secretKey: str, optional: bool })
    | TlsAuthentication({ enabled: bool })
    | NoneAuthentication          // no authentication configured :)

  type ResourceType = 
      topic     |
      group     |           // (abstracted out)
      cluster   |           // (abstracted out)
      transactionalId       // (abstracted out)

  type PatternType =
      literal |
      prefix                 // (abstracted out)

  type Operation =
      Read  |
      Write |                // (abstracted out)
      Create        |        // (abstracted out)
      Delete        |        // (abstracted out)
      Alter         |        // (abstracted out)
      Describe      |        // (abstracted out)
      ClusterAction |        // (abstracted out)
      AlterConfigs  |        // (abstracted out)
      DescribeConfigs |      // (abstracted out)
      IdempotentWrite |      // (abstracted out)
      All                    // (abstracted out)

  type AclResource = {
      name: str,
      typeAcl: ResourceType,
      patternType: PatternType
  }

  type SimpleAclRule = {
      resource: AclResource,
      operation: Operation,
      host: str,
      kind: str // "allow"
  }

  type KafkaUserAuthorization = {
      typeAuthz: str, // "simple"
      acls: Set[SimpleAclRule]
    }

  type KafkaUserQuotas = {
    producerByteRate: int,
    consumerByteRate: int,
    requestPercentage: int,
    controllerMutationRate: int
  }

  type KafkaUserSpec = {
    authentication: KafkaUserAuthentication,
    authorization: KafkaUserAuthorization,
    quotas: KafkaUserQuotas
  }

  type KafkaUserStatusCondition = {
  //  type: str,                  // (abstracted out)
    status: str,
  //  lastTransitionTime: str,    // (abstracted out)
  //  reason: str,                // (abstracted out)
  //  message: str                // (abstracted out)
  }

  type KafkaUserStatus = {
    conditions: KafkaUserStatusCondition // (abstracted out just one status not a list :) )
  //  observedGeneration: int,                    // (abstracted out)
  //  username: str,                              // (abstracted out)
  //  secret: str                                 // (abstracted out)
  }

  // A complete KafkaUser resource that holds both the desired spec and the actual account.
  type UserResource = {
    metadata: KafkaUserMetadata,
    spec: KafkaUserSpec,
    status: KafkaUserStatus  //   "Pending", "Ready", or "Deleted"
  }

  // ============================================================================
  // EVENT MODEL
  // ============================================================================
  type Event =
    | UserCreatedWithSecret(UserResource)
    | UserUpdated(UserResource)
    | UserDeleted(str)

  // ============================================================================
  // GLOBAL STATE
  // ============================================================================
  // GlobalState encapsulates the entire state of the operator.
  type GlobalState = {
    users: str -> UserResource,
    secrets: Set[str],
    eventQueue: List[Event],
    processedEvents: int
  }

  // ============================================================================
  // GLOBAL STATE VARIABLE
  // ============================================================================
  // All state is encapsulated in a single global variable.
  var globalState: GlobalState
  var parameters : {
    aclsEnabled : bool,
    potentialUsers : Set[str],
    maxProcessedEvents: int,
    authTypes: Set[str],
    quotasEnabled: Set[bool]
  }

  // ============================================================================
  // INITIALIZATION ACTION
  // ============================================================================
  action init = all {
    // we fix ACLs (i.e., enabled / disabled within one trace)
    nondet acl = Set(true, false).oneOf()

    // for verifying temporal property one has to set maxProcessedEvents <= 10 or verification would take 1 day ++ :)
    parameters' = {
        aclsEnabled : acl,
        potentialUsers : Set("Alice", "Bob", "Carol"),
        maxProcessedEvents : 1000,
        authTypes: Set("scram-sha-512", "tls", "none"),
        quotasEnabled: Set(true, false)
    },
    globalState' = {
      users: Map(),
      secrets : Set(),
      eventQueue: [],
      processedEvents: 0
    }
  }

  // ============================================================================
  // ACTIONS FOR GENERATING EVENTS
  // ============================================================================

  def requiresSecret(auth: KafkaUserAuthentication): bool =
  match auth {
    | ScramSha512Authentication(_) => true
    | TlsAuthentication(_) => true
    | NoneAuthentication => false
  }

  // helper for queueEvent
  def queueEvent(ev: Event): GlobalState =
    { ...globalState, eventQueue: globalState.eventQueue.append(ev) }

  def makeAuth(u: str, authType: str): KafkaUserAuthentication =
    if (authType == "scram-sha-512")
      ScramSha512Authentication({
        secretName: u,
        secretKey: "my-password",
        optional: false
      })
    else if (authType == "tls")
      TlsAuthentication({ enabled: true })
    else
      NoneAuthentication

  def makeQuotas(quotasEnabled: bool): KafkaUserQuotas =
    if (quotasEnabled == true)
      {
        producerByteRate: 100,
        consumerByteRate: 200,
        requestPercentage: 50,
        controllerMutationRate: 5
      }
    else
      {
        producerByteRate: 0,
        consumerByteRate: 0,
        requestPercentage: 0,
        controllerMutationRate: 0
      }

  def makeAcl(): Set[SimpleAclRule] = Set({
     resource: { name: "*", typeAcl: topic, patternType: literal },
     operation: Read,
     host: "*",
     kind: "allow"
  })

  def makeAuthz(enable: bool): KafkaUserAuthorization =
    if (enable)
        { typeAuthz: "simple", acls: makeAcl }
    else
        { typeAuthz: "disabled", acls: Set() }

  // Each action appends its corresponding event to the eventQueue.
  action createUser(u, authType, quotasEnabled, aclsEnabled) = all {
    globalState' = queueEvent(
      UserCreatedWithSecret({
        metadata: { name: u },
        spec: {
          authentication: makeAuth(u, authType),
          authorization: makeAuthz(aclsEnabled),
          quotas: makeQuotas(quotasEnabled)
        },
        status: { conditions: { status: "Pending" } }
      })
    )
  }

  action updateUser(u, authType, quotasEnabled, aclsEnabled) = all {
    if (u.in(globalState.users.keys())) {
      globalState' = queueEvent(
        UserUpdated({
          metadata: { name: u },
          spec: {
            authentication: makeAuth(u, authType),
            authorization: makeAuthz(aclsEnabled),
            quotas: makeQuotas(quotasEnabled)
          },
          status: { conditions: { status: "Pending" } }
        })
      )
    } else {
      globalState' = globalState
    }
  }

  action deleteUser(u) = all {
    if (u.in(globalState.users.keys())) {
      globalState' = queueEvent(UserDeleted(u))
    } else {
      globalState' = globalState
    }
  }

  action processNextEvent = all {
    if (length(globalState.eventQueue) > 0)
      globalState' = processEvent({ ...globalState, eventQueue: tail(globalState.eventQueue) }, head(globalState.eventQueue))
    else
      globalState' = globalState
  }

  // ============================================================================
  // STEP FUNCTION
  // ============================================================================
  // The step action nondeterministically chooses between generating an event and processing an event.
  // If the choice is "reconcile" but the eventQueue is empty, it falls back to generating an event.
  action step = all {
    nondet u = parameters.potentialUsers.oneOf()
    nondet authType = parameters.authTypes.oneOf()
    nondet quotasEnabled = parameters.quotasEnabled.oneOf()

    // the sum of (already processed + currently queued) is bounded.
    // we can never create more events than you will ever be allowed to process.
    // therefore, the system guaranteedly stops after maxProcessedEvents in total.
    if (globalState.processedEvents + length(globalState.eventQueue) < parameters.maxProcessedEvents)
      any {
        createUser(u, authType, quotasEnabled, parameters.aclsEnabled),
        updateUser(u, authType, quotasEnabled, parameters.aclsEnabled),
        deleteUser(u),
        processNextEvent
      }
    else
      processNextEvent,

    parameters' = parameters
  }

  // ============================================================================
  // RECONCILIATION LOGIC (UserControllerLoop)
  // ============================================================================
  def reconcileUser(user: UserResource, ev: Event): UserResource =
    match ev {
      | UserCreatedWithSecret(cr) =>
              {
                metadata: cr.metadata,
                spec: cr.spec,
                status: { conditions: { status: "Ready" } }
              }
      | UserUpdated(cr) =>
              if (user.status.conditions.status == "Deleted")
                user
              else
                {
                  metadata: cr.metadata,
                  spec: cr.spec,
                  status: {
                    conditions: {
                      // If the new authentication requires secret, only then check secret presence to become Ready.
                      // If the new authentication does NOT require secret, always directly become Ready.
                      status: if (requiresSecret(cr.spec.authentication))
                              (if (cr.metadata.name.in(globalState.secrets)) "Ready" else "Pending")
                              else
                                "Ready"
                    }
                  }
                }
      | UserDeleted(username) =>
              {
                metadata: user.metadata,
                spec: {
                  ...user.spec,
                  authorization: {
                    typeAuthz: "disabled",
                    acls: Set()
                  }
                },
                status: { conditions: { status: "Deleted" } }
              }
    }

  def processEvent(state: GlobalState, ev: Event): GlobalState =
    match ev {
      | UserCreatedWithSecret(cr) =>
        if (cr.metadata.name.in(state.users.keys()))
          {
            users: state.users.put(cr.metadata.name, reconcileUser(state.users.get(cr.metadata.name), ev)),
            secrets: if (requiresSecret(cr.spec.authentication)) state.secrets.union(Set(cr.metadata.name)) else state.secrets,
            eventQueue: state.eventQueue,
            processedEvents: state.processedEvents + 1
          }
        else
          {
            users: state.users.put(cr.metadata.name, {
              metadata: cr.metadata,
              spec: cr.spec,
              status: { conditions: { status: "Ready" } }
            }),
            secrets: if (requiresSecret(cr.spec.authentication)) state.secrets.union(Set(cr.metadata.name)) else state.secrets,
            eventQueue: state.eventQueue,
            processedEvents: state.processedEvents + 1
          }
      | UserUpdated(cr) =>
          if (cr.metadata.name.in(state.users.keys()))
            {
              users: state.users.put(cr.metadata.name, reconcileUser(state.users.get(cr.metadata.name), ev)),
              secrets:
                // Ignores updates to deleted users.
                // Doesn’t regenerate secrets for deleted users.
                if (state.users.get(cr.metadata.name).status.conditions.status == "Deleted")
                  state.secrets
                else
                  if (requiresSecret(cr.spec.authentication))
                    state.secrets.union(Set(cr.metadata.name))
                  else
                    state.secrets.exclude(Set(cr.metadata.name)),
              eventQueue: state.eventQueue,
              processedEvents: state.processedEvents + 1
            }
          else
            state
       | UserDeleted(username) =>
          if (username.in(state.users.keys()))
            {
              users: state.users.put(username, reconcileUser(state.users.get(username), ev)),
              secrets: state.secrets.exclude(Set(username)),
              eventQueue: state.eventQueue,
              processedEvents: state.processedEvents + 1
            }
          else
            state
  }

  // ========================================
  // ============== INVARIANTS ==============
  // ========================================

  // If Ready and requires Secret → Secret must exist
  // If Ready and does not require Secret → it’s OK if Secret is missing
  // If a user is "Deleted", no secret must exist for them.
  val UserConsistency = globalState.users.keys().forall(u =>
    ((globalState.users.get(u).status.conditions.status == "Ready") implies
      (not(requiresSecret(globalState.users.get(u).spec.authentication)) or globalState.secrets.contains(u))
    )
    and
    ((globalState.users.get(u).status.conditions.status == "Deleted") implies not(globalState.secrets.contains(u)))
  )

  // Only if Ready and Secret is needed → Secret must exist.
  // If a user is Ready and their current authentication requires a secret, then the secret must exist.
  val SecretsConsistency =
    globalState.users.keys().forall(u =>
      (requiresSecret(globalState.users.get(u).spec.authentication) and
       globalState.users.get(u).status.conditions.status == "Ready")
      implies globalState.secrets.contains(u)
    )

  // No user marked as "Deleted" should have a secret.
  // deletion imp/lies that a secret is *not required* anymore, but it may still exist
  val NoSecretsForDeletedUsers =
    globalState.users.keys().forall(u =>
      (globalState.users.get(u).status.conditions.status == "Deleted") implies
      not(globalState.secrets.contains(u))
    )

  // Every "Ready" user must have a corresponding secret.
  val ReadyUsersMustHaveSecrets =
    globalState.users.keys().forall(u =>
      (globalState.users.get(u).status.conditions.status == "Ready") implies
        (not(requiresSecret(globalState.users.get(u).spec.authentication)) or globalState.secrets.contains(u))
    )

  // Every event in the queue must refer to a known PotentialUser.
  val EventQueueWellFormed =
      length(globalState.eventQueue.select(ev =>
         match ev {
           | UserCreatedWithSecret(cr) => cr.metadata.name.in(parameters.potentialUsers)
           | UserUpdated(cr) => cr.metadata.name.in(parameters.potentialUsers)
           | UserDeleted(u)    => u.in(parameters.potentialUsers)
         }
       )) == length(globalState.eventQueue)

  // ========================================
  // ========= QUOTAS INVARIANTS ============
  // ========================================

  // All quota values must be non-negative.
  val QuotasNonNegative =
    globalState.users.keys().forall(u =>
      (globalState.users.get(u).spec.quotas.producerByteRate >= 0) and
      (globalState.users.get(u).spec.quotas.consumerByteRate >= 0) and
      (globalState.users.get(u).spec.quotas.requestPercentage >= 0) and
      (globalState.users.get(u).spec.quotas.controllerMutationRate >= 0)
    )

  // The requestPercentage quota must be between 0 and 100 inclusive.
  val QuotasRequestPercentageValid =
    globalState.users.keys().forall(u =>
      (globalState.users.get(u).spec.quotas.requestPercentage >= 0) and
      (globalState.users.get(u).spec.quotas.requestPercentage <= 100)
    )

  // If a KafkaUser is Ready, then their quotas must be valid. => (Non-negative and requestPercentage ∈ [0,100])
  val ReadyUsersQuotasValid =
    globalState.users.keys().forall(u =>
     (globalState.users.get(u).status.conditions.status == "Ready") implies
     (
       globalState.users.get(u).spec.quotas.producerByteRate >= 0
       and globalState.users.get(u).spec.quotas.consumerByteRate >= 0
       and globalState.users.get(u).spec.quotas.requestPercentage >= 0
       and globalState.users.get(u).spec.quotas.requestPercentage <= 100
       and globalState.users.get(u).spec.quotas.controllerMutationRate >= 0
     )
  )

  // ========================================
  // ====== AUTHENTICATION INVARIANTS =======
  // ========================================
  val AuthenticationPresent =
  globalState.users.keys().forall(u =>
    match globalState.users.get(u).spec.authentication {
      | ScramSha512Authentication(_) => true
      | TlsAuthentication(_) => true
      | NoneAuthentication => true
    }
  )

  // ========================================
  // =========== ACL INVARIANTS =============
  // ========================================

  // If a user’s authorization is enabled (typeAuthz == "simple" and ACLs are non-empty),
  // then each rule must have valid structure.
  val ACLsExistForAuthorizedUsers =
    globalState.users.keys().forall(u =>
      (globalState.users.get(u).spec.authorization.typeAuthz == "simple") implies
        globalState.users.get(u).spec.authorization.acls.forall(rule =>
          rule.resource.name != "" and
          rule.host != "" and
          rule.kind == "allow"
        )
    )

  // A deleted user must not have any ACLs.
  val NoACLsForDeletedUsers =
    globalState.users.keys().forall(u =>
      (globalState.users.get(u).status.conditions.status == "Deleted") implies
        globalState.users.get(u).spec.authorization.acls == Set()
    )

  // If a user is “Ready” and authorization is enabled, then they must have at least one ACL rule.
  val ReadyUsersMustHaveACLs =
    globalState.users.keys().forall(u =>
      (globalState.users.get(u).status.conditions.status == "Ready"
       and globalState.users.get(u).spec.authorization.typeAuthz == "simple")
      implies globalState.users.get(u).spec.authorization.acls.size() > 0
    )

  // ========================================
  // ========================================
  // ========================================

  val AllInvariantsHold =
    UserConsistency
     and SecretsConsistency
     and NoSecretsForDeletedUsers
     and ReadyUsersMustHaveSecrets
     and EventQueueWellFormed
     and QuotasNonNegative
     and QuotasRequestPercentageValid
     and ReadyUsersQuotasValid
     and AuthenticationPresent
     and ACLsExistForAuthorizedUsers
     and NoACLsForDeletedUsers
     and ReadyUsersMustHaveACLs

  // ========================================
  // ======== TEMPORAL PROPERTIES ===========
  // ========================================
  // A helper predicate that returns true if, for every potential user,
  // either the user is not present in the state or, if present, its status is "Ready" or "Deleted".
  def isStable(state: GlobalState): bool =
    parameters.potentialUsers.forall(u =>
      (not(u.in(state.users.keys()))) or
      (state.users.get(u).status.conditions.status == "Ready") or
      (state.users.get(u).status.conditions.status == "Deleted")
    )


  // ========================================
  // ========= TEMPORAL PROPERTIES ==========
  // ========================================

  // Temporal Property: Eventually Stable Reconciliation (ESR)
  // -----------------------------------------------------------
  // This property asserts that if the `step` action is executed in a strongly fair
  // manner (i.e., strongFair(step, globalState)), then the system must eventually
  // reach a *stable* state.
  //
  // A *stable* state means:
  //   1. The event queue is empty (i.e., no pending events remain), and
  //   2. Every potential user is either absent from the system or, if present,
  //      has a status of "Ready" or "Deleted" (i.e., is considered stable).
  //
  // However, to allow for models that intentionally bound the number of processed
  // events (to prevent infinite behavior during exploration), we additionally
  // permit termination once the `processedEvents` counter reaches `maxProcessedEvents`.
  //
  // In other words, *stability must eventually be reached*, unless the system
  // intentionally stops by exhausting the allowed number of processed events.
  temporal EventuallyStableReconciliationProperty =
    strongFair(step, globalState) implies always(eventually(
      (
        (length(globalState.eventQueue) == 0)
        and isStable(globalState)
      )
      or
      (globalState.processedEvents >= parameters.maxProcessedEvents)
    ))
}
