package it.unitn.disi.ds1.etc;

import akka.actor.ActorRef;
import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * {@link it.unitn.disi.ds1.actor.Actor} metadata.
 * It emulates a Pair.
 */
public final class ActorMetadata implements Serializable {
    private static final long serialVersionUID = -1491009620968209465L;

    /**
     * {@link it.unitn.disi.ds1.actor.Actor} id.
     */
    @Expose
    public final int id;

    /**
     * {@link it.unitn.disi.ds1.actor.Actor} {@link ActorRef ref}.
     */
    @Expose
    public final ActorRef ref;

    /**
     * Construct a new ActorMetadata class.
     *
     * @param id  Actor id
     * @param ref Actor ref
     */
    public ActorMetadata(int id, ActorRef ref) {
        this.id = id;
        this.ref = ref;
    }

    /**
     * Factory method for creating an ActorMetadata.
     *
     * @param id  Actor id
     * @param ref Actor ref
     * @return ActorMetadata instance
     */
    public static ActorMetadata of(int id, ActorRef ref) {
        return new ActorMetadata(id, ref);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ActorMetadata that = (ActorMetadata) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
        result = prime * result + (ref == null ? 0 : ref.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
