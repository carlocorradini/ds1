package it.unitn.disi.ds1;

import akka.actor.AbstractActor;
import akka.actor.Props;
import it.unitn.disi.ds1.message.*;

import java.util.*;
import java.util.stream.IntStream;

public final class Server extends AbstractActor {
    private final int serverId;
    private final HashMap<Integer, Item> dataStore;
    private final List<WriteRequest> workspace; //Better data structure Hashmap<UUID, ArrayList<WriteRequest>>?

    public Server(int serverId) {
        this.serverId = serverId;
        this.dataStore = new HashMap<>();
        this.workspace = new ArrayList<>();

        // Initialize server data
        IntStream.range(serverId * 10, (serverId * 10) + 10).forEach(i -> dataStore.put(i, new Item()));
    }

    public static Props props(int serverId) {
        return Props.create(Server.class, () -> new Server(serverId));
    }

    /*-- Actor methods -------------------------------------------------------- */
    private boolean checkVersion(UUID transactionId) {
        for (WriteRequest i : this.workspace) {
            if (i.transactionId.equals(transactionId) && i.actualVersion != this.dataStore.get(i.key).version) {
                return false;
            }
        }
        return true;
    }

    private void applyChanges(UUID transactionId) {
        for (WriteRequest i : this.workspace) {
            if (i.transactionId.equals(transactionId)) {
                this.dataStore.put(i.key, new Item(this.dataStore.get(i.key).version + 1, i.newValue));
            }
        }
    }

    /*-- Message handlers ----------------------------------------------------- */

    private void onReadCoordMsg(ReadCoordMsg msg) {
        getSender().tell(new ReadResultCoordMsg(msg.transactionId, msg.key, this.dataStore.get(msg.key).data), getSelf());
    }

    private void onWriteCoordMsg(WriteCoordMsg msg) {
        // Add write request to workspace
        this.workspace.add(new WriteRequest(msg.transactionId, msg.key, this.dataStore.get(msg.key).version, msg.value));
        // System.out.println(this.workspace);
    }

    private void onRequestMsg(RequestMsg msg) {
        if (msg.decision) {
            // Return to coordinator Yes or No
            System.out.printf("%s --> Reply to coordinator:%s\n", getSelf().path().name(), checkVersion(msg.transactionId));
            getSender().tell(new ResponseMsg(msg.transactionId, checkVersion(msg.transactionId)), getSender());
        } else {
            // Abort immediately
            System.out.printf("%s --> Client decides to ABORT immediately\n", getSelf().path().name());
            this.workspace.removeIf(i -> i.transactionId.equals(msg.transactionId));
        }
    }

    private void onDecisionMsg(DecisionMsg msg) {
        if (msg.decision) {
            // Apply changes if Commit
            applyChanges(msg.transactionId);
            System.out.printf("%s new values stored!\n%s\n", getSelf().path().name(), this.dataStore);
        }
        // Discard changes if Abort or Clear workspace
        this.workspace.removeIf(i -> i.transactionId.equals(msg.transactionId));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReadCoordMsg.class, this::onReadCoordMsg)
                .match(WriteCoordMsg.class, this::onWriteCoordMsg)
                .match(RequestMsg.class, this::onRequestMsg)
                .match(DecisionMsg.class, this::onDecisionMsg)
                .build();
    }
}
