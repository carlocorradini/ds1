package it.unitn.disi.ds1;

import akka.actor.AbstractActor;
import akka.actor.Props;

import java.util.HashMap;
import java.util.stream.IntStream;

public final class Server extends AbstractActor {
    private final int id;
    private final HashMap<Integer, Item> dataStore;

    public Server(int id) {
        this.id = id;
        this.dataStore = new HashMap<>();

        // Initialize server data
        IntStream.range(id * 10, (id * 10) + 10).forEach(i -> dataStore.put(i, new Item()));
        System.out.println(this.dataStore);
    }

    public static Props props(int id) {
        return Props.create(Server.class, () -> new Server(id));
    }

    /*-- Actor methods -------------------------------------------------------- */

    /*-- Message handlers ----------------------------------------------------- */
    @Override
    public Receive createReceive() {
        return receiveBuilder().build();
    }
}
