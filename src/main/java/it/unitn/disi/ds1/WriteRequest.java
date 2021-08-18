package it.unitn.disi.ds1;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.actor.DataStore;

import java.util.UUID;

/**
 * Write request stored in workspace by {@link DataStore}.
 */

public final class WriteRequest {
    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

    /**
     * Transaction ide.
     */
    @Expose
    public final UUID transactionId;

    /**
     * Item key.
     */
    @Expose
    public final int key;

    /**
     * Item actual version.
     */
    @Expose
    public final int actualVersion;

    /**
     * New requested Item value.
     */
    @Expose
    public final int newValue;

    /**
     * Construct an item with the given value and version.
     *
     * @param transactionId   Transaction id
     * @param key Item key
     * @param actualVersion Item actual version
     * @param newValue New item value
     */
    public WriteRequest(UUID transactionId, int key, int actualVersion, int newValue) {
        this.transactionId = transactionId;
        this.key = key;
        this.actualVersion = actualVersion;
        this.newValue = newValue;
    }

    @Override
    public String toString() { return GSON.toJson(this); }
}
