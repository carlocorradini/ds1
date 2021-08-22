package it.unitn.disi.ds1.message.snap;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * Message for a {@link it.unitn.disi.ds1.actor.Coordinator} asking to start a snapshot.
 */
public final class StartSnapshotMessage implements Serializable {
    private static final long serialVersionUID = -6783191695509349242L;

    /**
     * Snapshot id.
     */
    @Expose
    public final int snapshotId;

    /**
     * Construct a new StartSnapshotMessage class.
     *
     * @param snapshotId Snapshot id
     */
    public StartSnapshotMessage(int snapshotId) { this.snapshotId = snapshotId; }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
