package it.unitn.disi.ds1.message;

import java.io.Serializable;

/**
 * WRITE request from the client to the coordinator.
 */
public final class WriteMsg implements Serializable {
    private static final long serialVersionUID = 8248714506636891726L;

    public final int clientId;
    public final int key; // Key of the value to write
    public final int value; // New value to write

    public WriteMsg(int clientId, int key, int value) {
        this.clientId = clientId;
        this.key = key;
        this.value = value;
    }
}
