package com.github.segabriel.cluster;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.status.SystemCounterDescriptor;
import java.io.File;
import java.nio.file.Paths;
import java.util.function.Function;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterNode implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(ClusterNode.class);

  private final String instanceDirName;
  private final Contexts contexts;
  private final NodeChannelResolver node;
  private final NodeChannelResolver[] nodes;

  // State
  private final MediaDriver mediaDriver;
  private final Archive archive;
  private final ConsensusModule consensusModule;
  private final ClusteredServiceContainer clusteredServiceContainer;

  public static ClusterNode single(
      NodeChannelResolver node, Function<ClusterNode, ClusteredService> serviceSupplier) {
    return new ClusterNode(new NodeChannelResolver[] {node}, 0, serviceSupplier);
  }

  public ClusterNode(
      NodeChannelResolver[] nodes,
      int currentIdx,
      Function<ClusterNode, ClusteredService> serviceSupplier) {
    this.node = nodes[currentIdx];
    this.nodes = nodes;
    this.instanceDirName =
        Paths.get("target", "aeron", "node", Integer.toString(node.nodeId())).toString();
    this.contexts = new Contexts();

    logger.info("Starting node {} on {}, address: {}", this, instanceDirName, node);

    this.mediaDriver = MediaDriver.launch(contexts.mediaDriverCtx.clone());

    this.archive =
        Archive.launch(
            contexts
                .archiveCtx
                .clone()
                .mediaDriverAgentInvoker(mediaDriver.sharedAgentInvoker())
                .errorHandler(mediaDriver.context().errorHandler())
                .errorCounter(
                    mediaDriver.context().systemCounters().get(SystemCounterDescriptor.ERRORS)));

    this.consensusModule = ConsensusModule.launch(contexts.consensusModuleCtx.clone());

    ClusteredService clusteredService = serviceSupplier.apply(this);

    this.clusteredServiceContainer =
        ClusteredServiceContainer.launch(
            contexts.clusteredServiceContainerCtx.clone().clusteredService(clusteredService));
  }

  @Override
  public void close() {
    CloseHelper.close(clusteredServiceContainer);
    CloseHelper.close(consensusModule);
    CloseHelper.close(archive);
    CloseHelper.close(mediaDriver);
  }

  public void cleanUp() {
    IoUtil.delete(new File(instanceDirName), true);
  }

  private class Contexts {

    private final MediaDriver.Context mediaDriverCtx;
    private final Archive.Context archiveCtx;
    private final AeronArchive.Context aeronArchiveCtx;
    private final ConsensusModule.Context consensusModuleCtx;
    private final ClusteredServiceContainer.Context clusteredServiceContainerCtx;

    private Contexts() {

      mediaDriverCtx =
          new MediaDriver.Context()
              .aeronDirectoryName(Paths.get(instanceDirName, "media").toString())
              .spiesSimulateConnection(true)
              .threadingMode(ThreadingMode.SHARED)
              .dirDeleteOnStart(true)
              .errorHandler(ex -> logger.error("Exception occurred at MediaDriver: " + ex, ex));

      archiveCtx =
          new Archive.Context()
              .aeronDirectoryName(mediaDriverCtx.aeronDirectoryName())
              .archiveDir(new File(instanceDirName, "archive"))
              .threadingMode(ArchiveThreadingMode.SHARED)
              .localControlChannel("aeron:ipc?term-length=64k")
              .controlChannel(node.archiveControlRequestChannel())
              .controlStreamId(node.archiveControlRequestStreamId())
              .localControlStreamId(node.archiveControlRequestStreamId())
              .recordingEventsChannel(node.archiveRecordingEventsChannel())
              .errorHandler(ex -> logger.error("Exception occurred at Archive: " + ex, ex));

      aeronArchiveCtx =
          new AeronArchive.Context()
              .aeronDirectoryName(mediaDriverCtx.aeronDirectoryName())
              .controlRequestChannel(node.archiveControlRequestChannel())
              .controlRequestStreamId(node.archiveControlRequestStreamId())
              .controlResponseChannel(node.archiveControlResponseChannel())
              .controlResponseStreamId(node.archiveControlResponseStreamId())
              .recordingEventsChannel(node.archiveRecordingEventsChannel())
              .errorHandler(ex -> logger.error("Exception occurred at AeronArchive: " + ex, ex));

      consensusModuleCtx =
          new ConsensusModule.Context()
              .aeronDirectoryName(mediaDriverCtx.aeronDirectoryName())
              .clusterDir(new File(instanceDirName, "consensus"))
              .archiveContext(aeronArchiveCtx.clone())
              .clusterMemberId(node.nodeId())
              .clusterMembers(NodeChannelResolver.toClusterMembers(nodes))
              .ingressChannel("aeron:udp?term-length=64k")
              .logChannel(node.logChannel())
              .errorHandler(ex -> logger.error("Exception occurred at ConsensusModule: " + ex, ex));

      clusteredServiceContainerCtx =
          new ClusteredServiceContainer.Context()
              .aeronDirectoryName(mediaDriverCtx.aeronDirectoryName())
              .clusterDir(new File(instanceDirName, "service"))
              .archiveContext(aeronArchiveCtx.clone())
              .errorHandler(
                  ex -> logger.error("Exception occurred at ClusteredServiceContainer: " + ex, ex));
    }
  }
}
