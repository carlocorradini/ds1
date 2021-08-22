package it.unitn.disi.ds1.util.adapter.serializer;

import akka.actor.ActorRef;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * {@link JsonElement JSON} serializer for {@link ActorRef}.
 */
public final class ActorRefSerializer implements JsonSerializer<ActorRef> {
    @Override
    public JsonElement serialize(ActorRef src, Type typeOfSrc, JsonSerializationContext context) {
        if (src == null) return null;

        return new JsonPrimitive(src.toString());
    }
}
