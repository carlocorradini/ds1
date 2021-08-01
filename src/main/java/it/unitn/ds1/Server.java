package it.unitn.ds1;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

import akka.actor.*;

import it.unitn.ds1.Message.*;
import it.unitn.ds1.Item;

public class Server extends AbstractActor {

    private final Integer serverId;
    private HashMap<Integer, Item> dataStore = new HashMap<Integer, Item>();

    /*-- Actor constructor ---------------------------------------------------- */

    public Server(int serverId) {
        this.serverId = serverId;
        initData();        
    }

    static public Props props(int serverId) {
        return Props.create(Server.class, () -> new Server(serverId));
    }

    /*-- Actor methods -------------------------------------------------------- */

    void initData() {

        // Initialize server data
        for (int i=this.serverId*10; i<(this.serverId*10)+10; i++) {
            this.dataStore.put(i, new Item());
        }

        System.out.println(this.dataStore);
    }

    /*-- Message handlers ----------------------------------------------------- */

    @Override
    public Receive createReceive() {
      return receiveBuilder()
              .build();
    }
}
