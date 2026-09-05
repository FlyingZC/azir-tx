package mvcc.api;

import mvcc.common.IsolationLevel;
import mvcc.common.TxStatus;
import mvcc.storage.Mutation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

public final class Transaction {
    private final long txId;
    private final long startTs;
    private final IsolationLevel isolationLevel;
    private final Set<String> participants = new LinkedHashSet<>();
    private final Map<String, NavigableMap<String, Mutation>> bufferedMutationsByShard = new LinkedHashMap<>();
    private boolean metadataPersisted;
    private TxStatus status = TxStatus.ACTIVE;

    public Transaction(long txId, long startTs, IsolationLevel isolationLevel) {
        this.txId = txId;
        this.startTs = startTs;
        this.isolationLevel = isolationLevel;
    }

    public long txId() {
        return txId;
    }

    public long startTs() {
        return startTs;
    }

    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }

    public Set<String> participants() {
        return Collections.unmodifiableSet(participants);
    }

    public void addParticipant(String shardId) {
        participants.add(shardId);
    }

    public void bufferMutation(String shardId, Mutation mutation) {
        participants.add(shardId);
        bufferedMutationsByShard
                .computeIfAbsent(shardId, ignored -> new TreeMap<>())
                .put(mutation.key(), mutation);
    }

    public Optional<Mutation> bufferedMutation(String shardId, String key) {
        NavigableMap<String, Mutation> mutations = bufferedMutationsByShard.get(shardId);
        if (mutations == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(mutations.get(key));
    }

    public List<Mutation> bufferedMutations(String shardId) {
        NavigableMap<String, Mutation> mutations = bufferedMutationsByShard.get(shardId);
        if (mutations == null || mutations.isEmpty()) {
            return List.of();
        }
        return List.copyOf(mutations.values());
    }

    public List<Mutation> bufferedMutationsInRange(String shardId, String startKeyInclusive, String endKeyExclusive) {
        NavigableMap<String, Mutation> mutations = bufferedMutationsByShard.get(shardId);
        if (mutations == null || mutations.isEmpty()) {
            return List.of();
        }
        return new ArrayList<>(mutations.subMap(startKeyInclusive, true, endKeyExclusive, false).values());
    }

    public TxStatus status() {
        return status;
    }

    public void status(TxStatus status) {
        this.status = status;
    }

    public boolean metadataPersisted() {
        return metadataPersisted;
    }

    public void metadataPersisted(boolean metadataPersisted) {
        this.metadataPersisted = metadataPersisted;
    }
}
