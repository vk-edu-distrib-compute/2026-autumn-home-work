package company.vk.edu.distrib.compute.mperikov.kv;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.IntFunction;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;

final class KvCoordinator {
    private static final Executor LOCAL_IO = Thread::startVirtualThread;

    private final int selfPort;
    private final Dao<byte[]> entities;
    private final ReplicaClient remotes;

    KvCoordinator(int selfPort, Dao<byte[]> entities, HttpClient client) {
        this.selfPort = selfPort;
        this.entities = entities;
        remotes = new ReplicaClient(client);
    }

    void serve(HttpExchange exchange, String method, String id, int ack, byte[] body) throws IOException {
        if (KvCodes.GET.equals(method)) {
            read(exchange, id, ack);
            return;
        }
        if (KvCodes.PUT.equals(method)) {
            write(exchange, id, ack, body);
            return;
        }
        if (KvCodes.DELETE.equals(method)) {
            remove(exchange, id, ack);
            return;
        }
        KvHandler.sendEmpty(exchange, KvCodes.NOT_FOUND);
    }

    private void write(HttpExchange exchange, String id, int ack, byte[] body) {
        int copies = putReplicas(id, body, ack);
        int status = enough(copies, ack) ? KvCodes.CREATED : KvCodes.UNAVAILABLE;
        KvHandler.sendEmpty(exchange, status);
    }

    private void remove(HttpExchange exchange, String id, int ack) {
        int copies = deleteReplicas(id, ack);
        int status = enough(copies, ack) ? KvCodes.OK : KvCodes.UNAVAILABLE;
        KvHandler.sendEmpty(exchange, status);
    }

    private void read(HttpExchange exchange, String id, int ack) throws IOException {
        ReadVote.ReadResult vote = readReplicas(id, ack);
        if (enough(vote.answers(), ack) && vote.present()) {
            KvHandler.sendBytes(exchange, KvCodes.OK, vote.stored());
            return;
        }
        int status = enough(vote.answers(), ack) ? KvCodes.NOT_FOUND : KvCodes.UNAVAILABLE;
        KvHandler.sendEmpty(exchange, status);
    }

    private int putReplicas(String id, byte[] body, int ack) {
        return countAcks(id, ack, () -> writeLocal(id, body), port -> remotes.put(port, id, body));
    }

    private int deleteReplicas(String id, int ack) {
        return countAcks(id, ack, () -> deleteLocal(id), port -> remotes.delete(port, id));
    }

    private int countAcks(String id, int ack, BooleanSupplier local, IntFunction<CompletableFuture<Boolean>> remote) {
        int[] ports = replicas(id);
        if (ports.length == 0) {
            return 0;
        }
        AckGate gate = new AckGate(ports.length, ack);
        int localCopies = fanout(ports, gate, remote);
        for (int copy = 0; copy < localCopies; copy++) {
            CompletableFuture.supplyAsync(local::getAsBoolean, LOCAL_IO).whenComplete(gate::settle);
        }
        return gate.await();
    }

    private int fanout(int[] ports, AckGate gate, IntFunction<CompletableFuture<Boolean>> remote) {
        int localCopies = 0;
        for (int port : ports) {
            if (port == selfPort) {
                localCopies++;
                continue;
            }
            remote.apply(port).whenComplete(gate::settle);
        }
        return localCopies;
    }

    private boolean writeLocal(String id, byte[] body) {
        try {
            entities.upsert(id, body);
            return true;
        } catch (IOException ex) {
            ReplicaClient.miss(ex);
            return false;
        }
    }

    private boolean deleteLocal(String id) {
        try {
            entities.delete(id);
            return true;
        } catch (IOException ex) {
            ReplicaClient.miss(ex);
            return false;
        }
    }

    private ReadVote.ReadResult readReplicas(String id, int ack) {
        int[] ports = replicas(id);
        ReadVote vote = new ReadVote(ack, ports.length);
        int localCopies = 0;
        for (int port : ports) {
            if (port == selfPort) {
                localCopies++;
                continue;
            }
            remotes.readInto(vote, port, id);
        }
        for (int copy = 0; copy < localCopies; copy++) {
            readLocal(vote, id);
        }
        return vote.await();
    }

    private void readLocal(ReadVote vote, String id) {
        try {
            vote.remember(KvCodes.OK, entities.get(id));
        } catch (NoSuchElementException expected) {
            vote.remember(KvCodes.NOT_FOUND, new byte[0]);
        } catch (IOException ex) {
            ReplicaClient.miss(ex);
            vote.failed();
        }
    }

    private int[] replicas(String id) {
        int[] members = KvCodes.memberPorts();
        if (members.length == 0) {
            return new int[] {selfPort};
        }
        return KvCodes.replicaPorts(id, members);
    }

    private static boolean enough(int copies, int ack) {
        return copies >= ack;
    }
}
