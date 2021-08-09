package it.unitn.disi.ds1.message;

import java.io.Serializable;

/**
 * READ request from the client to the coordinator.
 */
public final class ReadMsg implements Serializable {
    private static final long serialVersionUID = 278859909154339067L;

    public final int clientId;
    public final int key; // Key of the value to read

    public ReadMsg(int clientId, int key) {
        this.clientId = clientId;
        this.key = key;
    }
}
