package it.unitn.disi.ds1.message.snapshot;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.message.Message;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * Snapshot request message
 * from {@link it.unitn.disi.ds1.Main} to {@link it.unitn.disi.ds1.actor.Coordinator}
 * or from {@link it.unitn.disi.ds1.actor.Coordinator} to {@link it.unitn.disi.ds1.actor.DataStore}.
 */
public final class SnapshotMessage extends Message implements Serializable {
    private static final long serialVersionUID = 5081090567329182418L;

    /**
     * Snapshot id.
     */
    @Expose
    public final int snapshotId;

    /**
     * Construct a new SnapshotMessage class.
     *
     * @param senderId   Sender {@link it.unitn.disi.ds1.actor.Actor} id
     * @param snapshotId Snapshot id
     */
    public SnapshotMessage(int senderId, int snapshotId) {
        super(senderId);
        this.snapshotId = snapshotId;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
