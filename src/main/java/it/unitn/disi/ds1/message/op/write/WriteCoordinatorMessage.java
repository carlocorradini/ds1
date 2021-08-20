package it.unitn.disi.ds1.message.op.write;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.message.txn.TxnMessage;

import java.util.UUID;

import java.io.Serializable;

/**
 * Write request message
 * from {@link it.unitn.disi.ds1.actor.Coordinator} to {@link it.unitn.disi.ds1.actor.DataStore}.
 */
public final class WriteCoordinatorMessage extends TxnMessage implements Serializable {
    private static final long serialVersionUID = -4823398098700891377L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

    /**
     * {@link Item} key to write.
     */
    @Expose
    public final int key;

    /**
     * New {@link Item} value to write.
     */
    @Expose
    public final int value;

    /**
     * Construct a new WriteCoordinatorMessage class.
     *
     * @param transactionId Transaction id
     * @param key           Item key to write
     * @param value         Item new value to write
     */
    public WriteCoordinatorMessage(UUID transactionId, int key, int value) {
        super(transactionId);
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
