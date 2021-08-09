package it.unitn.disi.ds1.message;

import java.io.Serializable;

/**
 * Reply from the coordinator when requested a READ on a given key.
 */
public final class ReadResultMsg implements Serializable {
    private static final long serialVersionUID = 6073342617515584698L;

    public final int key; // Key associated to the requested item
    public final int value; // Value found in the data store for that item

    public ReadResultMsg(int key, int value) {
        this.key = key;
        this.value = value;
    }
}
