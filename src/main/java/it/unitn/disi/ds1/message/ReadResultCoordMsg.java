package it.unitn.disi.ds1.message;

import java.util.UUID;

import java.io.Serializable;

/**
 * Reply from the server when requested a READ on a given key by the coordinator.
 */
public final class ReadResultCoordMsg extends TxnMsg implements Serializable {
    private static final long serialVersionUID = 2418188472950018347L;

    // Item key
    public final int key;
    // Item value
    public final int value;

    public ReadResultCoordMsg(UUID transactionId, int key, int value) {
        super(transactionId);
        this.key = key;
        this.value = value;
    }
}
