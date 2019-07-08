package com.github.segabriel.cluster;

import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.AeronCluster.AsyncConnect;
import io.aeron.cluster.client.AeronCluster.Context;
import io.aeron.cluster.client.EgressListener;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.BufferClaim;
import java.io.File;
import java.nio.file.Paths;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterClient implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(ClusterClient.class);

  private final String clientDirName;
  private final Contexts contexts;
  private final NodeChannelResolver client;
  private final NodeChannelResolver[] nodes;
  private final EgressListener egressListener;

  // State
  private final MediaDriver mediaDriver;
  private final AgentRunner agentRunner;
  private final ClusterClientAgent clientAgent;

  public ClusterClient(
      NodeChannelResolver client, NodeChannelResolver node, EgressListener egressListener) {
    this(client, new NodeChannelResolver[] {node}, egressListener);
  }

  public ClusterClient(
      NodeChannelResolver client, NodeChannelResolver[] nodes, EgressListener egressListener) {
    this.client = client;
    this.nodes = nodes;
    this.egressListener = egressListener;
    clientDirName =
        Paths.get("target", "aeron", "cluster-client", Integer.toString(this.hashCode()))
            .toString();
    this.contexts = new Contexts();

    logger.info("Starting client {} on {}", this, clientDirName);

    this.mediaDriver = MediaDriver.launch(contexts.mediaDriverCtx.clone());

    clientAgent = new ClusterClientAgent(contexts.cluserClientCtx);

    agentRunner =
        new AgentRunner(
            new YieldingIdleStrategy(),
            ex -> logger.error("Exception occurred at AeronCluster: " + ex, ex),
            null,
            clientAgent);

    AgentRunner.startOnThread(agentRunner);
  }

  @Override
  public void close() {
    CloseHelper.close(agentRunner);
    CloseHelper.close(mediaDriver);
  }

  public void cleanUp() {
    IoUtil.delete(new File(clientDirName), true);
  }

  public long offer(final DirectBuffer buffer, final int offset, final int length) {
    AeronCluster client = clientAgent.cluster;
    if (client == null) {
      return Publication.NOT_CONNECTED;
    }
    return client.offer(buffer, offset, length);
  }

  public long tryClaim(final int length, final BufferClaim bufferClaim) {
    AeronCluster client = clientAgent.cluster;
    if (client == null) {
      return Publication.NOT_CONNECTED;
    }
    return client.tryClaim(length, bufferClaim);
  }

  private class Contexts {

    private final MediaDriver.Context mediaDriverCtx;
    private final AeronCluster.Context cluserClientCtx;

    private Contexts() {
      mediaDriverCtx =
          new MediaDriver.Context()
              .aeronDirectoryName(Paths.get(clientDirName, "media").toString())
              .threadingMode(ThreadingMode.SHARED)
              .dirDeleteOnStart(true)
              .errorHandler(
                  ex -> logger.error("Exception occurred at client MediaDriver: " + ex, ex));

      cluserClientCtx =
          new AeronCluster.Context()
              .errorHandler(ex -> logger.error("Exception occurred at AeronCluster: " + ex, ex))
              .ingressChannel(client.clientIngressChannel())
              .egressChannel(client.clientEgressChannel())
              .aeronDirectoryName(mediaDriverCtx.aeronDirectoryName())
              .clusterMemberEndpoints(NodeChannelResolver.toClusterMemberEndpoints(nodes))
              .egressListener(egressListener);
    }
  }

  private class ClusterClientAgent implements Agent {

    private static final int SESSION_KEEP_ALIVE_INTERVAL_MILLIS = 1000;

    private final Context clientCtx;

    private AsyncConnect asyncConnect;
    private AeronCluster cluster;
    private EpochClock epochClock;

    private long sessionKeepAliveDeadline;

    public ClusterClientAgent(AeronCluster.Context clientCtx) {
      this.clientCtx = clientCtx;
    }

    @Override
    public void onStart() {
      logger.info("Connecting aeron-cluster on endpoints: {}", clientCtx.clusterMemberEndpoints());
    }

    @Override
    public int doWork() {
      int result = 0;
      if (cluster != null) {
        result = cluster.pollEgress();

        long now = epochClock.time();
        if (now >= sessionKeepAliveDeadline) {
          sessionKeepAliveDeadline = now + SESSION_KEEP_ALIVE_INTERVAL_MILLIS;
          cluster.sendKeepAlive();
        }
      } else {
        try {
          if (asyncConnect == null) {
            asyncConnect = AeronCluster.asyncConnect(clientCtx.clone());
          }

          cluster = asyncConnect.poll();
          if (cluster != null) {
            logger.info(
                "Connected aeron-cluster on endpoints: {}", clientCtx.clusterMemberEndpoints());
            epochClock = cluster.context().aeron().context().epochClock();
            sessionKeepAliveDeadline = epochClock.time() + SESSION_KEEP_ALIVE_INTERVAL_MILLIS;
            result = 1;
          }
        } catch (io.aeron.exceptions.TimeoutException ex) {
          logger.warn(
              "TimeoutException at connecting aeron-cluster on {}, reconnecting ...",
              clientCtx.clusterMemberEndpoints());
          asyncConnect = null;
          cluster = null;
        }
      }
      return result;
    }

    @Override
    public void onClose() {
      logger.info("Closing aeron-cluster on endpoints: {}", clientCtx.clusterMemberEndpoints());
      CloseHelper.quietClose(cluster);
      CloseHelper.quietClose(asyncConnect);
      logger.info("Closed aeron-cluster on endpoints: {}", clientCtx.clusterMemberEndpoints());
    }

    @Override
    public String roleName() {
      return getClass().getSimpleName();
    }
  }
}
