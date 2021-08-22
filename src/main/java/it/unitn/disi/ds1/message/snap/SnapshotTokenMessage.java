package it.unitn.disi.ds1.message.snap;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * Snapshot token request message
 * from {@link it.unitn.disi.ds1.actor.Coordinator} to {@link it.unitn.disi.ds1.actor.DataStore}.
 */
public final class SnapshotTokenMessage implements Serializable {
    private static final long serialVersionUID = 5081090567329182418L;

    /**
     * Coordinator id.
     */
    @Expose
    public final int coordinatorId;

    /**
     * Snapshot id.
     */
    @Expose
    public final int snapshotId;

    /**
     * Construct a new StartSnapshotMessage class.
     *
     * @param coordinatorId {@link it.unitn.disi.ds1.actor.Coordinator} id
     * @param snapshotId    Snapshot id
     */
    public SnapshotTokenMessage(int coordinatorId, int snapshotId) {
        this.coordinatorId = coordinatorId;
        this.snapshotId = snapshotId;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
