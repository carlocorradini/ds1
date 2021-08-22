package it.unitn.disi.ds1.etc;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.message.pc.two.TwoPcDecision;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * {@link it.unitn.disi.ds1.actor.DataStore} 2PC decision.
 * It emulates a Pair.
 */
public final class DataStoreDecision implements Serializable {
    private static final long serialVersionUID = 5068914277834770966L;

    /**
     * {@link it.unitn.disi.ds1.actor.DataStore} id.
     */
    @Expose
    public final int id;

    /**
     * Decision made.
     */
    @Expose
    public final TwoPcDecision decision;

    /**
     * Construct a new DataStoreDecision class.
     *
     * @param id       DataStore id
     * @param decision Decision
     */
    public DataStoreDecision(int id, TwoPcDecision decision) {
        this.id = id;
        this.decision = decision;
    }

    /**
     * Factory method for creating a DataStoreDecision.
     *
     * @param id       DataStore id
     * @param decision Decision
     * @return DataStoreDecision instance
     */
    public static DataStoreDecision of(int id, TwoPcDecision decision) {
        return new DataStoreDecision(id, decision);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataStoreDecision that = (DataStoreDecision) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
        result = prime * result + ((decision == null) ? 0 : decision.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
