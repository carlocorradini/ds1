package it.unitn.disi.ds1.message.snap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

import java.io.Serializable;

/**
 * Message for a {@link it.unitn.disi.ds1.actor.Coordinator} asking to start a snapshot.
 */
public final class StartSnapshotMessage implements Serializable {
    private static final long serialVersionUID = -6783191695509349242L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

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
        return GSON.toJson(this);
    }
}
