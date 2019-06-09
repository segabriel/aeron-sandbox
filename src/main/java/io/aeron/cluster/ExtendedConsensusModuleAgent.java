package io.aeron.cluster;

/** Workaround to access {@link ConsensusModuleAgent} */
public class ExtendedConsensusModuleAgent extends ConsensusModuleAgent {

  public ExtendedConsensusModuleAgent(ConsensusModule.Context ctx) {
    super(ctx);
  }
}
