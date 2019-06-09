package io.aeron.cluster.service;

/** Workaround to access {@link ClusteredServiceAgent} */
public class ExtendedClusteredServiceAgent extends ClusteredServiceAgent {

  public ExtendedClusteredServiceAgent(ClusteredServiceContainer.Context ctx) {
    super(ctx);
  }
}
