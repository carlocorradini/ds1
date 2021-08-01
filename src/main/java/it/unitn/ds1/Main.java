package it.unitn.ds1;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import it.unitn.ds1.Message.WelcomeMsg;

public class Main {
  final static int N_COORDINATORS = 2;
  final static int N_SERVERS = 3;
  final static int N_CLIENTS = 5;
  final static int MAX_ITEM = 29;  

  public static void main(String[] args) {
    // Create the actor system
    final ActorSystem system = ActorSystem.create("helloakka");

    // Create servers
    List<ActorRef> groupServer = new ArrayList<>();
    for (int i=0; i<N_SERVERS; i++) {
      groupServer.add(system.actorOf(Server.props(i), "Server" + i));
    }

    // Create coordinators
    List<ActorRef> groupCoord = new ArrayList<>();
    for (int i=0; i<N_COORDINATORS; i++) {
      groupCoord.add(system.actorOf(Coordinator.props(groupServer), "Coordinator" + i));
    }

    // Create clients
    List<ActorRef> groupClient = new ArrayList<>();
    for (int i=0; i<N_CLIENTS; i++) {
      groupClient.add(system.actorOf(TxnClient.props(i), "Client" + i));
    }

    // Send welcome message to clients from coordinators
    WelcomeMsg welcome = new WelcomeMsg(MAX_ITEM, groupCoord);
    for (ActorRef peer: groupClient) {
      peer.tell(welcome, ActorRef.noSender());
    }

    try {
      System.out.println(">>> Press ENTER to exit <<<");
      System.in.read();
    } 
    catch (IOException ignored) {}
    system.terminate();

  }
}
