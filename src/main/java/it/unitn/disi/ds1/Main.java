package it.unitn.disi.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.message.WelcomeMsg;

import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.stream.IntStream;

public final class Main {
    private final static int N_COORDINATORS = 2;
    private final static int N_SERVERS = 3;
    private final static int N_CLIENTS = 5;
    private final static int MAX_ITEM = 29;

    public static void main(String[] args) {
        final ActorSystem system;
        final List<ActorRef> servers = new ArrayList<>(N_SERVERS);
        final List<ActorRef> coords = new ArrayList<>(N_COORDINATORS);
        final List<ActorRef> clients = new ArrayList<>(N_CLIENTS);

        // Create actor system
        system = ActorSystem.create("helloakka");

        // Create servers
        IntStream.range(0, N_SERVERS).forEach(i -> servers.add(system.actorOf(Server.props(i), String.format("Server-%d", i))));

        // Create coordinators
        IntStream.range(0, N_COORDINATORS).forEach(i -> coords.add(system.actorOf(Coordinator.props(servers), String.format("Coordinator-%d", i))));

        // Create clients
        IntStream.range(0, N_CLIENTS).forEach(i -> clients.add(system.actorOf(TxnClient.props(i), String.format("Client-%d", i))));

        // Send welcome message to clients from coordinators
        WelcomeMsg welcome = new WelcomeMsg(MAX_ITEM, coords);
        clients.forEach(peer -> peer.tell(welcome, ActorRef.noSender()));

        // Wait until `ENTER` key
        System.out.println(">>> Press ENTER to exit <<<");
        new Scanner(System.in).nextLine();
        system.terminate();
    }
}
