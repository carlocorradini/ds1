package it.unitn.disi.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.actor.Client;
import it.unitn.disi.ds1.actor.Coordinator;
import it.unitn.disi.ds1.actor.DataStore;
import it.unitn.disi.ds1.message.welcome.ClientWelcomeMessage;
import it.unitn.disi.ds1.message.welcome.CoordinatorWelcomeMessage;
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

    /**
     * Logger.
     */
    private static final Logger LOGGER = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        // --- Variables ---
        final ActorSystem system = ActorSystem.create("banky");
        final List<ActorRef> dataStores = new ArrayList<>(N_DATA_STORES);
        final List<ActorRef> coordinators = new ArrayList<>(N_COORDINATORS);
        final List<ActorRef> clients = new ArrayList<>(N_CLIENTS);

        // --- Initialization ---
        // Data stores
        LOGGER.info("Initializing {} data stores", N_DATA_STORES);
        IntStream.range(0, N_DATA_STORES).forEach(i -> dataStores.add(system.actorOf(DataStore.props(i))));
        // Coordinators
        LOGGER.info("Initializing {} coordinators", N_COORDINATORS);
        IntStream.range(0, N_COORDINATORS).forEach(i -> coordinators.add(system.actorOf(Coordinator.props(i))));
        // Clients
        LOGGER.info("Initializing {} clients", N_CLIENTS);
        IntStream.range(0, N_CLIENTS).forEach(i -> clients.add(system.actorOf(Client.props(i))));

        // --- Welcome ---
        // Coordinators
        final CoordinatorWelcomeMessage coordinatorWelcomeMessage = new CoordinatorWelcomeMessage(dataStores);
        coordinators.forEach(coordinator -> coordinator.tell(coordinatorWelcomeMessage, ActorRef.noSender()));
        // Clients
        final ClientWelcomeMessage clientWelcomeMessage = new ClientWelcomeMessage(coordinators, MAX_ITEM_KEY);
        clients.forEach(client -> client.tell(clientWelcomeMessage, ActorRef.noSender()));

        // --- Run ---
        // Wait until `ENTER` key
        System.out.println(">>> Press ENTER to exit <<<");
        new Scanner(System.in).nextLine();
        system.terminate();
    }
}
