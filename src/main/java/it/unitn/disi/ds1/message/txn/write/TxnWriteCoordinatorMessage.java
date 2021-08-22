package it.unitn.disi.ds1.message.txn.write;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.message.txn.TxnMessage;
import it.unitn.disi.ds1.util.JsonUtil;

import java.util.UUID;

import java.io.Serializable;

/**
 * Write message
 * from {@link it.unitn.disi.ds1.actor.Coordinator} to {@link it.unitn.disi.ds1.actor.DataStore}.
 */
public final class TxnWriteCoordinatorMessage extends TxnMessage implements Serializable {
    private static final long serialVersionUID = -4823398098700891377L;

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
     * Construct a new TxnWriteCoordinatorMessage class.
     *
     * @param coordinatorId Coordinator id
     * @param transactionId Transaction id
     * @param key           Item key to write
     * @param value         Item new value to write
     */
    public TxnWriteCoordinatorMessage(int coordinatorId, UUID transactionId, int key, int value) {
        super(coordinatorId, transactionId);
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
