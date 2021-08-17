package it.unitn.disi.ds1.message.welcome;

import akka.actor.ActorRef;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.adapter.serializer.ActorRefSerializer;

import java.io.Serializable;
import java.util.List;

/**
 * {@link it.unitn.disi.ds1.actor.Coordinator Coordinator(s)} welcome message.
 */
public final class CoordinatorWelcomeMessage implements Serializable {
    private static final long serialVersionUID = -5587829195464438106L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .registerTypeAdapter(ActorRef.class, new ActorRefSerializer())
            .create();

    /**
     * List of available {@link it.unitn.disi.ds1.actor.DataStore DataStore(s)}.
     */
    @Expose
    public final List<ActorRef> dataStores;

    /**
     * Construct a new CoordinatorWelcomeMessage class.
     *
     * @param dataStores List of available {@link it.unitn.disi.ds1.actor.DataStore DataStore(s)}
     */
    public CoordinatorWelcomeMessage(List<ActorRef> dataStores) {
        this.dataStores = List.copyOf(dataStores);
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
