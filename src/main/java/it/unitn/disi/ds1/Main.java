package it.unitn.disi.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.actor.Client;
import it.unitn.disi.ds1.actor.Coordinator;
import it.unitn.disi.ds1.actor.DataStore;
import it.unitn.disi.ds1.etc.ActorMetadata;
import it.unitn.disi.ds1.message.welcome.ClientWelcomeMessage;
import it.unitn.disi.ds1.message.welcome.CoordinatorWelcomeMessage;
import it.unitn.disi.ds1.message.welcome.DataStoreWelcomeMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.IntStream;

/**
 * Main class.
 */
public final class Main {
    /**
     * Number of {@link DataStore Data Store(s)}.
     */
    private final static int N_DATA_STORES = 3;

    /**
     * Number of {@link Coordinator Coordinator(s)}.
     */
    private final static int N_COORDINATORS = 2;

    /**
     * Number of {@link Client Client(s)}.
     */
    private final static int N_CLIENTS = 1;

    /**
     * Maximum item key index value.
     */
    private final static int MAX_ITEM_KEY = (N_DATA_STORES * 10) - 1;

    /**
     * Logger.
     */
    private static final Logger LOGGER = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        // --- Variables ---
        final ActorSystem system = ActorSystem.create("banky");
        final List<ActorMetadata> dataStores = new ArrayList<>(N_DATA_STORES);
        final List<ActorMetadata> coordinators = new ArrayList<>(N_COORDINATORS);
        final List<ActorMetadata> clients = new ArrayList<>(N_CLIENTS);

        // --- Initialization ---
        // Data stores
        LOGGER.info("Initializing {} data stores", N_DATA_STORES);
        IntStream.range(0, N_DATA_STORES).forEach(id -> dataStores.add(ActorMetadata.of(id, system.actorOf(DataStore.props(id)))));
        // Coordinators
        LOGGER.info("Initializing {} coordinators", N_COORDINATORS);
        IntStream.range(0, N_COORDINATORS).forEach(id -> coordinators.add(ActorMetadata.of(id, system.actorOf(Coordinator.props(id)))));
        // Clients
        LOGGER.info("Initializing {} clients", N_CLIENTS);
        IntStream.range(0, N_CLIENTS).forEach(id -> clients.add(ActorMetadata.of(id, system.actorOf(Client.props(id)))));

        // --- Welcome ---
        // DataStores
        final DataStoreWelcomeMessage dataStoreWelcomeMessage = new DataStoreWelcomeMessage(dataStores);
        dataStores.forEach(dataStore -> dataStore.ref.tell(dataStoreWelcomeMessage, ActorRef.noSender()));
        // Coordinators
        final CoordinatorWelcomeMessage coordinatorWelcomeMessage = new CoordinatorWelcomeMessage(dataStores);
        coordinators.forEach(coordinator -> coordinator.ref.tell(coordinatorWelcomeMessage, ActorRef.noSender()));
        // Clients
        final ClientWelcomeMessage clientWelcomeMessage = new ClientWelcomeMessage(coordinators, MAX_ITEM_KEY);
        clients.forEach(client -> client.ref.tell(clientWelcomeMessage, ActorRef.noSender()));

        // --- Run ---
        // Wait until `ENTER` key
        System.out.println(">>> Press ENTER to exit <<<");
        new Scanner(System.in).nextLine();
        system.terminate();
    }
}
