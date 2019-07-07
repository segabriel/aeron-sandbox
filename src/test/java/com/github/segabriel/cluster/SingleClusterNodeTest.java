package com.github.segabriel.cluster;

import java.nio.charset.Charset;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;

class SingleClusterNodeTest {

  private ClusterNode clusterNode;
  private ClusterClient clusterClient;

  private ExpandableArrayBuffer buffer;
  private IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(100);

  @BeforeEach
  void setUp() {
    final NodeChannelResolver cluster = new NodeChannelResolver(7000);
    final NodeChannelResolver client = new NodeChannelResolver(8000);

    clusterNode = ClusterNode.single(cluster, i -> new StubClusteredService());

    clusterClient = new ClusterClient(client, cluster, new StubEgressListener());

    buffer = new ExpandableArrayBuffer();
  }

  @AfterEach
  void tearDown() {
    CloseHelper.close(clusterClient);
    CloseHelper.close(clusterNode);
  }

  @RepeatedTest(30)
  void testClientSendAndForget() {
    send("Hello world");
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
}
