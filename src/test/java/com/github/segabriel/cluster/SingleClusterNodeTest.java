package com.github.segabriel.cluster;

import static com.github.segabriel.cluster.NodeChannelResolver.CLIENT_REPLAY_CHANNEL_PORT_OFFSET;
import static com.github.segabriel.cluster.NodeChannelResolver.CLIENT_REPLAY_STREAM_ID;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Image;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.logbuffer.Header;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SingleClusterNodeTest {

  private static final Logger logger = LoggerFactory.getLogger(SingleClusterNodeTest.class);

  private static final int CLIENT_PORT = 8000;
  private static final int NODE_PORT = 7000;

  private ClusterNode clusterNode;
  private ClusterClient clusterClient;

  private ExpandableArrayBuffer buffer;
  private IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(100);

  @BeforeEach
  void setUp() {
    final NodeChannelResolver cluster = new NodeChannelResolver(NODE_PORT);
    final NodeChannelResolver client = new NodeChannelResolver(CLIENT_PORT);

    clusterNode =
        ClusterNode.single(
            cluster, node -> new ClusteredServiceImpl(node.contexts().aeronArchiveCtx.clone()));

    clusterClient = new ClusterClient(client, cluster, new StubEgressListener());

    buffer = new ExpandableArrayBuffer();
  }

  @AfterEach
  void tearDown() {
    CloseHelper.close(clusterClient);
    CloseHelper.close(clusterNode);
    if (clusterNode != null) {
      clusterNode.cleanUp();
    }
  }

  @RepeatedTest(30)
  void testClientSendAndForget() throws Exception {
    send("Hello world");

    TimeUnit.MILLISECONDS.sleep(250);
  }

  private void send(String msg) {
    byte[] bytes = msg.getBytes(Charset.defaultCharset());
    buffer.putBytes(0, bytes);

    while (true) {
      long result = clusterClient.offer(buffer, 0, bytes.length);
      if (result > 0) {
        break;
      }
      idleStrategy.idle((int) result);
    }
  }

  private static class ClusteredServiceImpl extends StubClusteredService {

    private static final String REPLAY_CHANNEL =
        new ChannelUriStringBuilder()
            .media(CommonContext.UDP_MEDIA)
            .reliable(true)
            .endpoint("localhost:" + (CLIENT_PORT + CLIENT_REPLAY_CHANNEL_PORT_OFFSET))
            .build();

    private final AeronArchive.Context aeronArchiveCtx;

    private final UnsafeBuffer offerBuffer = new UnsafeBuffer(new byte[1024]);

    private Cluster cluster;
    private AeronArchive aeronArchive;

    private Long recordingId;

    public ClusteredServiceImpl(AeronArchive.Context aeronArchiveCtx) {
      this.aeronArchiveCtx = aeronArchiveCtx;
    }

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
      super.onStart(cluster, snapshotImage);
      this.cluster = cluster;
      Aeron aeron =
          Aeron.connect(
              new Aeron.Context()
                  .aeronDirectoryName(aeronArchiveCtx.aeronDirectoryName())
                  .errorHandler(aeronArchiveCtx.errorHandler()));
      this.aeronArchive =
          AeronArchive.connect(
              aeronArchiveCtx
                  .clone()
                  .aeron(aeron)
                  .ownsAeronClient(true)
                  .idleStrategy(new ClusteredIdleStrategy(cluster)));
    }

    @Override
    public void onSessionMessage(
        ClientSession session,
        long timestampMs,
        DirectBuffer buffer,
        int offset,
        int length,
        Header header) {
      super.onSessionMessage(session, timestampMs, buffer, offset, length, header);

      if (cluster.memberId() == -1) {
        return;
      }

      if (recordingId == null) {
        aeronArchive.listRecordings(
            0,
            1,
            (controlSessionId,
                correlationId,
                recordingId,
                startTimestamp,
                stopTimestamp,
                startPosition,
                stopPosition,
                initialTermId,
                segmentFileLength,
                termBufferLength,
                mtuLength,
                sessionId,
                streamId,
                strippedChannel,
                originalChannel,
                sourceIdentity) -> this.recordingId = recordingId);
      }
      Objects.requireNonNull(recordingId);
      long replaySessionId =
          aeronArchive.startReplay(recordingId, 0, -1, REPLAY_CHANNEL, CLIENT_REPLAY_STREAM_ID);
      logger.info("replaySessionId: {}, for recordingId: {}", replaySessionId, recordingId);

      offerBuffer.putLong(0, replaySessionId);
      session.offer(offerBuffer, 0, Long.BYTES);
    }

    @Override
    public void onTerminate(Cluster cluster) {
      super.onTerminate(cluster);

      CloseHelper.close(aeronArchive);
    }
  }

  private static class ClusteredIdleStrategy implements IdleStrategy {

    private final Cluster cluster;

    ClusteredIdleStrategy(Cluster cluster) {
      this.cluster = cluster;
    }

    @Override
    public void idle(int workCount) {
      cluster.idle(workCount);
      // todo remove it after apply a new version aeron
      AgentInvoker agentInvoker = cluster.aeron().conductorAgentInvoker();
      if (agentInvoker != null) {
        agentInvoker.invoke();
      }
    }

    @Override
    public void idle() {
      cluster.idle();
      // todo remove it after apply a new version aeron
      AgentInvoker agentInvoker = cluster.aeron().conductorAgentInvoker();
      if (agentInvoker != null) {
        agentInvoker.invoke();
      }
    }

    @Override
    public void reset() {
      // no-op
    }
  }
}
