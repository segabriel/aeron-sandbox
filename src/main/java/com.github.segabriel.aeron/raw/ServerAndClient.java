package com.github.segabriel.aeron.raw;

public class ServerAndClient {

  public static void main(String[] args) {
    AeronResources aeronResources = AeronResources.start();

    new Server(aeronResources).start();

    new Client(aeronResources).start();

  }
}
