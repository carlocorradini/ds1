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
        Iterator<WriteRequest> it = this.workspace.iterator();
        while (it.hasNext()) {
            if (it.next().transactionId.equals(transactionId) && it.next().actualVersion != this.dataStore.get(it.next().key).version) {
                return false;
            }
        }
        return true;
    }

    /*-- Message handlers ----------------------------------------------------- */

    private void onReadCoordMsg(ReadCoordMsg msg) {
        getSender().tell(new ReadResultCoordMsg(msg.transactionId, msg.key, this.dataStore.get(msg.key).data), getSelf());
    }

    private void onWriteCoordMsg(WriteCoordMsg msg) {
        // Add write request to workspace
        this.workspace.add(new WriteRequest(msg.transactionId, msg.key, this.dataStore.get(msg.key).version, msg.value));
        System.out.println(this.workspace);
    }

    private void onDecisionMsg(RequestMsg msg) {
        if (msg.decision) {
            // Return to coordinator Yes or No
            getSender().tell(new ResponseMsg(msg.transactionId, checkVersion(msg.transactionId)), getSender());
        } else {
            // Abort immediately
            this.workspace.removeIf(i -> i.transactionId.equals(msg.transactionId));
        }

    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReadCoordMsg.class, this::onReadCoordMsg)
                .match(WriteCoordMsg.class, this::onWriteCoordMsg)
                .match(RequestMsg.class,  this::onDecisionMsg)
                .build();
    }
}
