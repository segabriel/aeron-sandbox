package com.github.segabriel.cluster;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.cluster.ConsensusModule.Configuration;
import io.aeron.cluster.ConsensusModule.Context;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class NodeChannelResolver {

  public static final int LOG_CHANNEL_PORT_OFFSET = 1;
  public static final int ARCHIVE_CONTROL_REQUEST_CHANNEL_PORT_OFFSET = 2;
  public static final int ARCHIVE_CONTROL_RESPONSE_CHANNEL_PORT_OFFSET = 3;
  public static final int ARCHIVE_RECORDING_EVENTS_CHANNEL_PORT_OFFSET = 4;
  public static final int SERVICE_CLIENT_FACING_PORT_OFFSET = 5;
  public static final int MEMBER_FACTING_PORT_OFFSET = 6;
  public static final int LOG_PORT_OFFSET = 7;
  public static final int TRANSFER_PORT_OFFSET = 8;
  public static final int INGRESS_CHANNEL_PORT_OFFSET = 9;
  public static final int EGRESS_CHANNEL_PORT_OFFSET = 10;

  public static final int ARCHIVE_CONTROL_REQUEST_STREAM_ID_OFFSET = 100;
  public static final int ARCHIVE_CONTROL_RESPONSE_STREAM_ID_OFFSET = 110;

  public static final int CLIENT_REPLAY_CHANNEL_PORT_OFFSET = 11;
  public static final int CLIENT_REPLAY_STREAM_ID = 120;

  private final String host;
  private final int port;

  /** See for details {@link Context#clusterMembers(String)}. */
  public static String toClusterMembers(NodeChannelResolver... clusterMembers) {
    return Arrays.stream(clusterMembers)
        .map(NodeChannelResolver::asString)
        .collect(Collectors.joining("|"));
  }

  /** See {@link io.aeron.cluster.client.AeronCluster.Context#clusterMemberEndpoints(String)}. */
  public static String toClusterMemberEndpoints(NodeChannelResolver... endpoints) {
    StringJoiner joiner = new StringJoiner(",");
    Arrays.stream(endpoints)
        .forEach(
            node ->
                joiner.add(
                    new StringBuilder()
                        .append(node.nodeId())
                        .append('=')
                        .append(node.host)
                        .append(':')
                        .append(node.port + SERVICE_CLIENT_FACING_PORT_OFFSET)));
    return joiner.toString();
  }

  public NodeChannelResolver(int port) {
    this("localhost", port);
  }

  public NodeChannelResolver(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public String logChannelAddress() {
    return host + ":" + (port + LOG_CHANNEL_PORT_OFFSET);
  }

  public String archiveControlRequestChannelAddress() {
    return host + ":" + (port + ARCHIVE_CONTROL_REQUEST_CHANNEL_PORT_OFFSET);
  }

  public String archiveControlResponseChannelAddress() {
    return host + ":" + (port + ARCHIVE_CONTROL_RESPONSE_CHANNEL_PORT_OFFSET);
  }

  public String archiveRecordingEventsChannelAddress() {
    return host + ":" + (port + ARCHIVE_RECORDING_EVENTS_CHANNEL_PORT_OFFSET);
  }

  public int archiveControlRequestStreamId() {
    return port + ARCHIVE_CONTROL_REQUEST_STREAM_ID_OFFSET;
  }

  public int archiveControlResponseStreamId() {
    return port + ARCHIVE_CONTROL_RESPONSE_STREAM_ID_OFFSET;
  }

  public String logChannel() {
    return new ChannelUriStringBuilder()
        .media("udp")
        .reliable(true)
        .controlMode("manual")
        .controlEndpoint(logChannelAddress())
        .build();
  }

  public String archiveControlRequestChannel() {
    return new ChannelUriStringBuilder()
        .media("udp")
        .reliable(true)
        .endpoint(archiveControlRequestChannelAddress())
        .build();
  }

  public String archiveControlResponseChannel() {
    return new ChannelUriStringBuilder()
        .media("udp")
        .reliable(true)
        .endpoint(archiveControlResponseChannelAddress())
        .build();
  }

  public String archiveRecordingEventsChannel() {
    return new ChannelUriStringBuilder()
        .media("udp")
        .reliable(true)
        .controlMode("dynamic")
        .controlEndpoint(archiveRecordingEventsChannelAddress())
        .build();
  }

  public String clientIngressChannel() {
    return new ChannelUriStringBuilder()
        .media("udp")
        .reliable(true)
        .endpoint(host + ":" + (port + INGRESS_CHANNEL_PORT_OFFSET))
        .build();
  }

  public String clientEgressChannel() {
    return new ChannelUriStringBuilder()
        .media("udp")
        .reliable(true)
        .endpoint(host + ":" + (port + EGRESS_CHANNEL_PORT_OFFSET))
        .build();
  }

  public String clientReplayChannel() {
    return new ChannelUriStringBuilder()
        .media(CommonContext.UDP_MEDIA)
        .reliable(true)
        .endpoint(host + ":" + (port + CLIENT_REPLAY_CHANNEL_PORT_OFFSET))
        .build();
  }

  public int clientReplayChannelStreamId() {
    return port + CLIENT_REPLAY_STREAM_ID;
  }

  public int nodeId() {
    return Integer.hashCode(port);
  }

  @Override
  public String toString() {
    return "@" + nodeId() + "-" + host + ":" + port;
  }

  /**
   * See for details {@link Configuration#CLUSTER_MEMBERS_PROP_NAME}
   *
   * @return cluster members string in aeron cluster format.
   */
  private String asString() {
    return String.valueOf(nodeId())
        + ','
        + host
        + ':'
        + (port + SERVICE_CLIENT_FACING_PORT_OFFSET)
        + ','
        + host
        + ':'
        + (port + MEMBER_FACTING_PORT_OFFSET)
        + ','
        + host
        + ':'
        + (port + LOG_PORT_OFFSET)
        + ','
        + host
        + ':'
        + (port + TRANSFER_PORT_OFFSET)
        + ','
        + host
        + ':'
        + (port + ARCHIVE_CONTROL_REQUEST_CHANNEL_PORT_OFFSET);
  }
}
