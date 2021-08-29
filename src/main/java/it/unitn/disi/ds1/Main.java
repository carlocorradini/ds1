package it.unitn.disi.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.actor.Client;
import it.unitn.disi.ds1.actor.Coordinator;
import it.unitn.disi.ds1.actor.DataStore;
import it.unitn.disi.ds1.etc.ActorMetadata;
import it.unitn.disi.ds1.message.Message;
import it.unitn.disi.ds1.message.snapshot.SnapshotMessage;
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
     * Logger.
     */
    private static final Logger LOGGER = LogManager.getLogger(Main.class);

    /**
     * Random instance.
     */
    private static final Random RANDOM = new Random();

    /**
     * Scanner instance.
     */
    private static final Scanner SCANNER = new Scanner(System.in);

    public static void main(String[] args) {
        // --- Variables ---
        final ActorSystem system = ActorSystem.create("banky");
        final List<ActorMetadata> dataStores = new ArrayList<>(Config.N_DATA_STORES);
        final List<ActorMetadata> coordinators = new ArrayList<>(Config.N_COORDINATORS);
        final List<ActorMetadata> clients = new ArrayList<>(Config.N_CLIENTS);

        // --- Initialization ---
        // Data stores
        LOGGER.info("Initializing {} data stores", Config.N_DATA_STORES);
        IntStream.range(0, Config.N_DATA_STORES).forEach(id -> dataStores.add(ActorMetadata.of(id, system.actorOf(DataStore.props(id)))));
        // Coordinators
        LOGGER.info("Initializing {} coordinators", Config.N_COORDINATORS);
        IntStream.range(0, Config.N_COORDINATORS).forEach(id -> coordinators.add(ActorMetadata.of(id, system.actorOf(Coordinator.props(id)))));
        // Clients
        LOGGER.info("Initializing {} clients", Config.N_CLIENTS);
        IntStream.range(0, Config.N_CLIENTS).forEach(id -> clients.add(ActorMetadata.of(id, system.actorOf(Client.props(id)))));

        // --- Welcome ---
        // DataStores
        final DataStoreWelcomeMessage dataStoreWelcomeMessage = new DataStoreWelcomeMessage(dataStores);
        dataStores.forEach(dataStore -> dataStore.ref.tell(dataStoreWelcomeMessage, ActorRef.noSender()));
        // Coordinators
        final CoordinatorWelcomeMessage coordinatorWelcomeMessage = new CoordinatorWelcomeMessage(dataStores);
        coordinators.forEach(coordinator -> coordinator.ref.tell(coordinatorWelcomeMessage, ActorRef.noSender()));

        // --- Run ---
        int run = 0;
        boolean continueTxn;

        // Lifecycle
        do {
            // Welcome Clients
            final ClientWelcomeMessage clientWelcomeMessage = new ClientWelcomeMessage(coordinators, Config.MAX_ITEM_KEY);
            clients.forEach(client -> client.ref.tell(clientWelcomeMessage, ActorRef.noSender()));

            // Mode selector
            switch (Config.MODE) {
                case INTERACTIVE: {
                    // Ask transaction question always
                    askTxnQuestion(coordinators, run);

                    // Continue ?
                    System.out.println("--- CONTINUE [Y|N]? ");
                    continueTxn = SCANNER.nextLine().matches("(?i)^(?:y(?:es)?|1)$");
                    break;
                }
                case AUTOMATIC: {
                    // Stop
                    continueTxn = false;
                    // Ask transaction question before terminating
                    askTxnQuestion(coordinators, 0);
                    break;
                }
                default: {
                    continueTxn = false;
                    break;
                }
            }

            // Increment run
            run += 1;
        } while (continueTxn);


        // Terminate
        LOGGER.info("Terminating system...");
        system.terminate();
    }

    /**
     * Utility to ask simple transaction question to the user.
     *
     * @param coordinators List of available {@link Coordinator Coordinator(s)} {@link ActorMetadata}
     * @param run          Run counter
     */
    public static void askTxnQuestion(List<ActorMetadata> coordinators, int run) {
        // Flush STDOUT
        System.out.flush();

        // Wait ...
        System.out.println("--- AFTER ALL TRANSACTION(S) PRESS `ENTER` TO CONTINUE...");
        SCANNER.nextLine();

        // Verify storage ?
        System.out.println("--- VERIFY STORAGE [Y|N]? ");
        final boolean verifyStorage = SCANNER.nextLine().matches("(?i)^(?:y(?:es)?|1)$");
        if (verifyStorage) {
            coordinators
                    .get(RANDOM.nextInt(coordinators.size()))
                    .ref.tell(new SnapshotMessage(Message.NO_SENDER_ID, run), ActorRef.noSender());
        }
    }
}
