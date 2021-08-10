package it.unitn.disi.ds1.message;

import java.io.Serializable;
import java.util.UUID;

/**
 * Reply from the coordinator when requested a READ on a given key.
 */
public final class ReadResultMsg extends TxnMsg implements Serializable {
    private static final long serialVersionUID = 6073342617515584698L;

    public final int key; // Key associated to the requested item
    public final int value; // Value found in the data store for that item

    public ReadResultMsg(UUID transactionId, int key, int value) {
        super(transactionId);
        this.key = key;
        this.value = value;
    }
}
