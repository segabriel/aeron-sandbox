package com.github.segabriel.cluster;

import io.aeron.archive.Archive;
import io.aeron.archive.Archive.Configuration;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MinMulticastFlowControlSupplier;
import io.aeron.driver.ThreadingMode;
import java.io.File;
import java.nio.file.Paths;
import org.agrona.IoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterNodeRunner {

  private static final Logger logger = LoggerFactory.getLogger(ClusterNodeRunner.class);

  public static void main(String[] args) throws Exception {

    int instanceId = Integer.getInteger("instanceId", Integer.MIN_VALUE);

    final String nodeDirName =
        Paths.get(IoUtil.tmpDirName(), "aeron", "cluster", "example", "" + instanceId).toString();
    IoUtil.delete(new File(nodeDirName), true);
    logger.info("Cluster node directory: {}", nodeDirName);

    String aeronDirectoryName = Paths.get(nodeDirName, "media-" + instanceId).toString();

    final MediaDriver.Context mediaDriverCtx =
        new MediaDriver.Context()
            .errorHandler(
                ex -> logger.error("Exception occurred MediaDriver[{}]: ", instanceId, ex))
            .aeronDirectoryName(aeronDirectoryName)
            .threadingMode(ThreadingMode.SHARED)
            .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
            .spiesSimulateConnection(true);

    final Archive.Context archiveCtx =
        new Archive.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .errorHandler(ex -> logger.error("Exception occurred Archive[{}]: ", instanceId, ex))
            .maxCatalogEntries(Configuration.maxCatalogEntries())
            .threadingMode(ArchiveThreadingMode.SHARED)
            .archiveDir(new File(nodeDirName, "archive-" + instanceId));

    final AeronArchive.Context aeronArchiveCtx =
        new AeronArchive.Context()
            .errorHandler(
                ex -> logger.error("Exception occurred AeronArchive[{}]: ", instanceId, ex))
            .aeronDirectoryName(aeronDirectoryName)
            .controlRequestChannel(archiveCtx.controlChannel())
            .controlRequestStreamId(archiveCtx.controlStreamId())
            .recordingEventsChannel(archiveCtx.recordingEventsChannel())
            .recordingEventsStreamId(archiveCtx.recordingEventsStreamId());

    final ConsensusModule.Context consensusModuleCtx =
        new ConsensusModule.Context()
            .errorHandler(
                ex -> logger.error("Exception occurred ConsensusModule[{}]: ", instanceId, ex))
            .aeronDirectoryName(aeronDirectoryName)
            .clusterDir(new File(nodeDirName, "consensus-module-" + instanceId))
            .archiveContext(aeronArchiveCtx.clone());

    final ClusteredServiceContainer.Context clusteredServiceContainerCtx =
        new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .errorHandler(
                ex -> logger.error("Exception occurred ServiceContainer[{}]: ", instanceId, ex))
            .archiveContext(aeronArchiveCtx.clone())
            .clusterDir(new File(nodeDirName, "service-" + instanceId))
            .clusteredService(new StubClusteredService(instanceId));

    try (ClusteredMediaDriver clusteredMediaDriver =
            ClusteredMediaDriver.launch(
                mediaDriverCtx.clone(), archiveCtx.clone(), consensusModuleCtx.clone());
        ClusteredServiceContainer clusteredServiceContainer =
            ClusteredServiceContainer.launch(clusteredServiceContainerCtx.clone())) {

      Thread.currentThread().join();
    }
  }
}
