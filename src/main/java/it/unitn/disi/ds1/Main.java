package it.unitn.disi.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.actor.Client;
import it.unitn.disi.ds1.actor.Coordinator;
import it.unitn.disi.ds1.actor.DataStore;
import it.unitn.disi.ds1.message.WelcomeMsg;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.stream.IntStream;

public final class Main {
    private final static int N_COORDINATORS = 2;
    private final static int N_DATA_STORES = 3;
    private final static int N_CLIENTS = 1;
    private final static int MAX_ITEM_KEY = 29;

    private static final Logger LOGGER = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        final ActorSystem system;
        final List<ActorRef> servers = new ArrayList<>(N_DATA_STORES);
        final List<ActorRef> coords = new ArrayList<>(N_COORDINATORS);
        final List<ActorRef> clients = new ArrayList<>(N_CLIENTS);

        // Create actor system
        system = ActorSystem.create("banky");

        // Initialize data stores
        LOGGER.info("Initializing {} data stores", N_DATA_STORES);
        IntStream.range(0, N_DATA_STORES).forEach(i -> servers.add(system.actorOf(DataStore.props(i), String.format("Server-%d", i))));

        // Initialize coordinators
        LOGGER.info("Initializing {} coordinators", N_COORDINATORS);
        IntStream.range(0, N_COORDINATORS).forEach(i -> coords.add(system.actorOf(Coordinator.props(i, servers), String.format("Coordinator-%d", i))));

        // Initialize clients
        LOGGER.info("Initializing {} clients", N_CLIENTS);
        IntStream.range(0, N_CLIENTS).forEach(i -> clients.add(system.actorOf(Client.props(i), String.format("Client-%d", i))));

        // Send welcome message to clients from coordinators
        WelcomeMsg welcome = new WelcomeMsg(coords, MAX_ITEM_KEY);
        clients.forEach(peer -> peer.tell(welcome, ActorRef.noSender()));

        // Wait until `ENTER` key
        System.out.println(">>> Press ENTER to exit <<<");
        new Scanner(System.in).nextLine();
        system.terminate();
    }
}
