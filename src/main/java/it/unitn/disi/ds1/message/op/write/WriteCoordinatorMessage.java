package it.unitn.disi.ds1.message.op.write;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.message.txn.TxnMessage;
import it.unitn.disi.ds1.util.JsonUtil;

import java.util.UUID;

import java.io.Serializable;

/**
 * Write request message
 * from {@link it.unitn.disi.ds1.actor.Coordinator} to {@link it.unitn.disi.ds1.actor.DataStore}.
 */
public final class WriteCoordinatorMessage extends TxnMessage implements Serializable {
    private static final long serialVersionUID = -4823398098700891377L;

    /**
     * Coordinator id.
     */
    @Expose
    public final int coordinatorId;

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
     * @param coordinatorId Coordinator id
     * @param transactionId Transaction id
     * @param key           Item key to write
     * @param value         Item new value to write
     */
    public WriteCoordinatorMessage(int coordinatorId, UUID transactionId, int key, int value) {
        super(transactionId);
        this.coordinatorId = coordinatorId;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
