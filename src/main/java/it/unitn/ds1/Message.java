package it.unitn.ds1;

import java.io.Serializable;
import java.util.*;

import akka.actor.*;

public class Message {

    // send this message to the client at startup to inform it about the coordinators and the keys
    public static class WelcomeMsg implements  Serializable {
        public final Integer maxKey;
        public final List<ActorRef> coordinators;
        public WelcomeMsg(int maxKey, List<ActorRef> coordinators) {
        this.maxKey = maxKey;
        this.coordinators = Collections.unmodifiableList(new ArrayList<>(coordinators));
        }
    }

    // stop the client
    public static class StopMsg implements Serializable {}

    // message the client sends to a coordinator to begin the TXN
    public static class TxnBeginMsg implements Serializable {
        public final Integer clientId;
        public TxnBeginMsg(int clientId) {
        this.clientId = clientId;
        }
    }

    // reply from the coordinator receiving TxnBeginMsg
    public static class TxnAcceptMsg implements Serializable {}

    // the client may timeout waiting for TXN begin confirmation (TxnAcceptMsg)
    public static class TxnAcceptTimeoutMsg implements Serializable {}

    // message the client sends to a coordinator to end the TXN;
    // it may ask for commit (with probability COMMIT_PROBABILITY), or abort
    public static class TxnEndMsg implements Serializable {
        public final Integer clientId;
        public final Boolean commit; // if false, the transaction should abort
        public TxnEndMsg(int clientId, boolean commit) {
        this.clientId = clientId;
        this.commit = commit;
        }
    }

    // READ request from the client to the coordinator
    public static class ReadMsg implements Serializable {
        public final Integer clientId;
        public final Integer key; // the key of the value to read
        public ReadMsg(int clientId, int key) {
        this.clientId = clientId;
        this.key = key;
        }
    }

    // WRITE request from the client to the coordinator
    public static class WriteMsg implements Serializable {
        public final Integer clientId;
        public final Integer key; // the key of the value to write
        public final Integer value; // the new value to write
        public WriteMsg(int clientId, int key, int value) {
        this.clientId = clientId;
        this.key = key;
        this.value = value;
        }
    }

    // reply from the coordinator when requested a READ on a given key
    public static class ReadResultMsg implements Serializable {
        public final Integer key; // the key associated to the requested item
        public final Integer value; // the value found in the data store for that item
        public ReadResultMsg(int key, int value) {
        this.key = key;
        this.value = value;
        }
    }

    // message from the coordinator to the client with the outcome of the TXN
    public static class TxnResultMsg implements Serializable {
        public final Boolean commit; // if false, the transaction was aborted
        public TxnResultMsg(boolean commit) {
        this.commit = commit;
        }
    }
}
