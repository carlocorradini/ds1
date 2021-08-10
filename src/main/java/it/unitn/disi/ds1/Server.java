package it.unitn.disi.ds1;

import akka.actor.AbstractActor;
import akka.actor.Props;
import it.unitn.disi.ds1.message.ReadCoordMsg;
import it.unitn.disi.ds1.message.ReadResultCoordMsg;

import java.util.HashMap;
import java.util.stream.IntStream;

public final class Server extends AbstractActor {
    private final int serverId;
    private final HashMap<Integer, Item> dataStore;

    public Server(int serverId) {
        this.serverId = serverId;
        this.dataStore = new HashMap<>();

        // Initialize server data
        IntStream.range(serverId * 10, (serverId * 10) + 10).forEach(i -> dataStore.put(i, new Item()));
        System.out.println(this.dataStore);
    }

    public static Props props(int id) {
        return Props.create(Server.class, () -> new Server(id));
    }

    /*-- Actor methods -------------------------------------------------------- */

    /*-- Message handlers ----------------------------------------------------- */

    private void onReadCoordMsg(ReadCoordMsg msg) {
        getSender().tell(new ReadResultCoordMsg(msg.transactionId, msg.key, this.dataStore.get(msg.key).data), getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReadCoordMsg.class, this::onReadCoordMsg)
                .build();
    }
}
