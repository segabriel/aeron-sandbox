package com.github.segabriel.cluster;

import static io.aeron.driver.Configuration.IDLE_MAX_PARK_NS;
import static io.aeron.driver.Configuration.IDLE_MAX_SPINS;
import static io.aeron.driver.Configuration.IDLE_MAX_YIELDS;
import static io.aeron.driver.Configuration.IDLE_MIN_PARK_NS;

import io.aeron.archive.Archive;
import io.aeron.archive.Archive.Configuration;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.ExtendedConsensusModuleAgent;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.cluster.service.ExtendedClusteredServiceAgent;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.MinMulticastFlowControlSupplier;
import io.aeron.driver.ThreadingMode;
import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.DynamicCompositeAgent;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterExample {

  private static final Logger logger = LoggerFactory.getLogger(ClusterExample.class);

  public static void main(String[] args) throws Exception {
    final String nodeDirName =
        Paths.get(IoUtil.tmpDirName(), "aeron", "cluster", "example").toString();
    IoUtil.delete(new File(nodeDirName), true);
    logger.info("Cluster node directory: {}", nodeDirName);

    final int size = 2;
    final boolean sharedMediaDriver = false;

    final String[] aeronDirectoryNames = new String[size];
    for (int i = 0; i < size; i++) {
      String dir = "media-" + (sharedMediaDriver ? 0 : i);
      aeronDirectoryNames[i] = Paths.get(nodeDirName, dir).toString();
    }

    final MediaDriver.Context[] mediaDriverCtx = new MediaDriver.Context[size];
    for (int i = 0; i < size; i++) {
      final int j = i;
      mediaDriverCtx[j] =
          new MediaDriver.Context()
              .errorHandler(ex -> logger.error("Exception occurred MediaDriver[{}]: ", j, ex))
              .aeronDirectoryName(aeronDirectoryNames[j])
              .threadingMode(ThreadingMode.SHARED)
              .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
              .spiesSimulateConnection(true);
    }

    final Archive.Context[] archiveCtx = new Archive.Context[size];
    for (int i = 0; i < size; i++) {
      final int j = i;
      archiveCtx[j] =
          new Archive.Context()
              .aeronDirectoryName(aeronDirectoryNames[j])
              .errorHandler(ex -> logger.error("Exception occurred Archive[{}]: ", j, ex))
              .idleStrategySupplier(ClusterExample::defaultIdleStrategy)
              .maxCatalogEntries(Configuration.maxCatalogEntries())
              .archiveDir(new File(nodeDirName, "archive-" + j))
              .controlChannel("aeron:udp?endpoint=localhost:801" + j)
              .controlStreamId(10100 + j)
              .recordingEventsChannel("aeron:udp?control-mode=dynamic|control=localhost:803" + j)
              .recordingEventsStreamId(10300 + j)
              .threadingMode(ArchiveThreadingMode.INVOKER);
    }

    final AeronArchive.Context[] aeronArchiveCtx = new AeronArchive.Context[size];
    for (int i = 0; i < size; i++) {
      final int j = i;
      aeronArchiveCtx[j] =
          new AeronArchive.Context()
              .errorHandler(ex -> logger.error("Exception occurred AeronArchive[{}]: ", j, ex))
              .aeronDirectoryName(aeronDirectoryNames[j])
              .controlRequestChannel(archiveCtx[j].controlChannel())
              .controlRequestStreamId(archiveCtx[j].controlStreamId())
              .controlResponseChannel("aeron:udp?endpoint=localhost:802" + j)
              .controlResponseStreamId(10200 + j)
              .recordingEventsChannel(archiveCtx[j].recordingEventsChannel())
              .recordingEventsStreamId(archiveCtx[j].recordingEventsStreamId());
    }

    final ConsensusModule.Context[] consensusModuleCtx = new ConsensusModule.Context[size];
    for (int i = 0; i < size; i++) {
      final int j = i;
      consensusModuleCtx[j] =
          new ConsensusModule.Context()
              .errorHandler(ex -> logger.error("Exception occurred ConsensusModule[{}]: ", j, ex))
              .idleStrategySupplier(ClusterExample::defaultIdleStrategy)
              .aeronDirectoryName(aeronDirectoryNames[j])
              .clusterDir(new File(nodeDirName, "consensus-module-" + j))
              .archiveContext(aeronArchiveCtx[j].clone())
              .ingressChannel("aeron:udp?term-length=64k")
              .logChannel(
                  "aeron:udp?term-length=256k|control-mode=manual|control=localhost:903" + j)
              .clusterMemberId(j)
              .clusterMembers(clusterMembersString(size));
    }

    final ClusteredServiceContainer.Context[] clusteredServiceContainerCtx =
        new ClusteredServiceContainer.Context[size];
    for (int i = 0; i < size; i++) {
      final int j = i;
      clusteredServiceContainerCtx[j] =
          new ClusteredServiceContainer.Context()
              .aeronDirectoryName(aeronDirectoryNames[j])
              .errorHandler(ex -> logger.error("Exception occurred ServiceContainer[{}]: ", j, ex))
              .idleStrategySupplier(ClusterExample::defaultIdleStrategy)
              .archiveContext(aeronArchiveCtx[j].clone())
              .clusterDir(new File(nodeDirName, "service-" + j))
              .clusteredService(new StubClusteredService(j));
    }

    MediaDriver[] mediaDrivers = new MediaDriver[size];
    Agent[] archiveAgents = new Agent[size];
    Agent[] consensusAgents = new Agent[size];
    Agent[] serviceAgents = new Agent[size];
    AgentRunner archiveRunner = null;
    AgentRunner consensusRunner = null;
    AgentRunner serviceRunner = null;
    try {

      if (sharedMediaDriver) {
        Context ctx = mediaDriverCtx[0].clone();
        MediaDriver mediaDriver = MediaDriver.launch(ctx);
        Arrays.fill(mediaDrivers, mediaDriver);
      } else {
        for (int i = 0; i < size; i++) {
          Context ctx = mediaDriverCtx[i].clone();
          mediaDrivers[i] = MediaDriver.launch(ctx);
        }
      }

      for (int i = 0; i < size; i++) {
        Archive.Context ctx = archiveCtx[i].clone();
        Agent agent = Archive.launch(ctx).invoker().agent();
        archiveAgents[i] = agent;
      }

      for (int i = 0; i < size; i++) {
        ConsensusModule.Context ctx = consensusModuleCtx[i].clone();
        try {
          ctx.conclude();
          consensusAgents[i] = new ExtendedConsensusModuleAgent(ctx);
        } catch (final Throwable ex) {
          if (null != ctx.clusterMarkFile()) {
            ctx.clusterMarkFile().signalFailedStart();
          }
          ctx.close();
          throw ex;
        }
      }

      for (int i = 0; i < size; i++) {
        ClusteredServiceContainer.Context ctx = clusteredServiceContainerCtx[i].clone();
        try {
          ctx.conclude();
          serviceAgents[i] = new ExtendedClusteredServiceAgent(ctx);
        } catch (final Throwable ex) {
          if (null != ctx.clusterMarkFile()) {
            ctx.clusterMarkFile().signalFailedStart();
          }
          ctx.close();
          throw ex;
        }
      }

      archiveRunner =
          new AgentRunner(
              defaultIdleStrategy(),
              ex -> logger.error("Exception occurred archiveRunner: ", ex),
              null,
              new DynamicCompositeAgent("composite-archive", archiveAgents));

      consensusRunner =
          new AgentRunner(
              defaultIdleStrategy(),
              ex -> logger.error("Exception occurred consensusRunner: ", ex),
              null,
              new DynamicCompositeAgent("composite-consensus", consensusAgents));

      serviceRunner =
          new AgentRunner(
              defaultIdleStrategy(),
              ex -> logger.error("Exception occurred serviceRunner: ", ex),
              null,
              new DynamicCompositeAgent("composite-service", serviceAgents));

      AgentRunner.startOnThread(archiveRunner);
      AgentRunner.startOnThread(consensusRunner);
      AgentRunner.startOnThread(serviceRunner);

      Thread.currentThread().join();

    } finally {

      if (serviceRunner != null) {
        CloseHelper.quietClose(serviceRunner);
      } else {
        for (Agent agent : serviceAgents) {
          try {
            agent.onClose();
          } catch (Exception ignore) {
          }
        }
      }

      if (consensusRunner != null) {
        CloseHelper.quietClose(consensusRunner);
      } else {
        for (Agent agent : consensusAgents) {
          try {
            agent.onClose();
          } catch (Exception ignore) {
          }
        }
      }

      if (archiveRunner != null) {
        CloseHelper.quietClose(archiveRunner);
      } else {
        for (Agent agent : archiveAgents) {
          try {
            agent.onClose();
          } catch (Exception ignore) {
          }
        }
      }

      for (MediaDriver mediaDriver : mediaDrivers) {
        if (mediaDriver != null) {
          CloseHelper.quietClose(mediaDriver);
        }
      }
    }
  }

  private static IdleStrategy defaultIdleStrategy() {
    return new BackoffIdleStrategy(
        IDLE_MAX_SPINS, IDLE_MAX_YIELDS, IDLE_MIN_PARK_NS, IDLE_MAX_PARK_NS);
  }

  private static String clusterMembersString(int size) {
    final StringBuilder builder = new StringBuilder();
    for (int i = 0; i < size; i++) {
      builder
          .append(i)
          .append(',')
          .append("localhost:2011")
          .append(i)
          .append(',')
          .append("localhost:2022")
          .append(i)
          .append(',')
          .append("localhost:2033")
          .append(i)
          .append(',')
          .append("localhost:2044")
          .append(i)
          .append(',')
          .append("localhost:801")
          .append(i)
          .append('|');
    }
    builder.setLength(builder.length() - 1);
    return builder.toString();
  }
}
