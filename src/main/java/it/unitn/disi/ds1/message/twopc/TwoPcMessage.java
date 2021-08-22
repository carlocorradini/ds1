package it.unitn.disi.ds1.message.twopc;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.etc.Decision;
import it.unitn.disi.ds1.message.txn.TxnMessage;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;
import java.util.UUID;

/**
 * General abstract message for 2PC.
 */
public abstract class TwoPcMessage extends TxnMessage implements Serializable {
    private static final long serialVersionUID = -7137752446196816824L;

    /**
     * Decision made.
     */
    @Expose
    public final Decision decision;

    /**
     * Construct a new TwoPCMessage class.
     *
     * @param senderId      {@link it.unitn.disi.ds1.actor.Actor Sender} id
     * @param transactionId Transaction id
     * @param decision      Decision
     */
    public TwoPcMessage(int senderId, UUID transactionId, Decision decision) {
        super(senderId, transactionId);
        this.decision = decision;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
