/*
 * Copyright 2026 Azir TX contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flyingzc.azir.wal.ha.example;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.wal.WalMutation;
import com.flyingzc.azir.wal.WalReceipt;
import com.flyingzc.azir.wal.ha.RatisManagedNode;
import com.flyingzc.azir.wal.ha.RatisTransaction;

import java.nio.ByteBuffer;

import java.util.zip.CRC32C;

/**
 * Cross-shard Transfer Example: The business unit number is the same as the balance transaction commit and does not guarantee external payment or message side effects.
 */
public final class IdempotentTransfer {

    private final RatisManagedNode node;

    private final String ledgerShard;

    /**
     * The caller is responsible for assigning the same route to all nodes, and tags and account data must be accessed through this mode.
     *
     * @param node node
     * @param ledgerShard ledger shard
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public IdempotentTransfer(final RatisManagedNode node, final String ledgerShard) {
        this.node = Objects.requireNonNull(node);
        new WalMutation(ledgerShard, "", null);
        if (ledgerShard.contains("\u0000")) {
            throw new IllegalArgumentException("invalid ledger shard");
        }
        this.ledgerShard = ledgerShard;
    }

    /**
     * Initialization allows only new accounts, and none of any accounts in the same transaction already exist.
     *
     * @param balances balances
     * @return the operation result
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public WalReceipt initializeAccounts(final Map<Account, Long> balances) throws Exception {
        var values = Map.copyOf(balances);
        if (values.isEmpty() || values.values().stream().anyMatch(value -> value < 0)) {
            throw new IllegalArgumentException("invalid initial balances");
        }
        return node.execute(IsolationLevel.REPEATABLE_READ, transaction -> {
            var view = transaction.statement();
            for (var account : values.keySet()) {
                if (view.get(account.shard(), accountKey(account)).isPresent()) {
                    throw new IllegalStateException("account already exists");
                }
            }
            for (var entry : values.entrySet()) {
                transaction.put(entry.getKey().shard(), accountKey(entry.getKey()), balanceBytes(entry.getValue()));
            }
            return null;
        }).receipt();
    }

    /**
     * There is no automatic retry: an explicit conflict can be re-referenced by the caller to the same business unit number and the unknown result cannot be replaced with a new single
     * number.
     *
     * @param request request
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    public Result transfer(final Request request) throws Exception {
        Objects.requireNonNull(request);
        var execution = node.execute(IsolationLevel.REPEATABLE_READ, transaction -> {
            var view = transaction.statement();
            var prior = view.get(ledgerShard, ledgerKey(request.operation()));
            if (prior.isPresent()) {
                var applied = decode(prior.get());
                if (!applied.request().equals(request)) {
                    throw new IllegalArgumentException("operation ID already used for a different request");
                }
                return applied;
            }
            long from = balance(view, request.from()), to = balance(view, request.to());
            if (from < request.amount()) {
                throw new IllegalArgumentException("insufficient balance");
            }
            var applied = new Applied(request, transaction.id(), from - request.amount(), Math.addExact(to, request.amount()));
            transaction.put(request.from().shard(), accountKey(request.from()), balanceBytes(applied.fromBalance()));
            transaction.put(request.to().shard(), accountKey(request.to()), balanceBytes(applied.toBalance()));
            transaction.put(ledgerShard, ledgerKey(request.operation()), encode(applied));
            return applied;
        });
        return new Result(execution.value(), execution.durableDecision() ? Optional.of(execution.receipt()) : Optional.empty());
    }

    /**
     * Query for business tags; missing only means that the current snapshot is not visible and does not allow the assertion of an unknown transaction already rollback.
     *
     * @param operation operation
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    public Optional<Applied> find(final UUID operation) throws Exception {
        Objects.requireNonNull(operation);
        return node.execute(IsolationLevel.REPEATABLE_READ, transaction -> {
            var bytes = transaction.get(ledgerShard, ledgerKey(operation));
            if (bytes.isEmpty()) {
                return Optional.<Applied>empty();
            }
            var value = decode(bytes.get());
            if (!value.request().operation().equals(operation)) {
                throw new IOException("transfer marker key mismatch");
            }
            return Optional.of(value);
        }).value();
    }

    /**
     * Sample point reading; multiple accounts should be audited in the same transaction and ReadView.
     *
     * @param account account
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    public long balance(final Account account) throws Exception {
        return node.execute(IsolationLevel.REPEATABLE_READ, transaction -> balance(transaction.statement(), account)).value();
    }

    /**
     * Strictly read a fixed, eight-byte non-negative balance and missing accounts cannot be treated in secret by zero.
     *
     * @param view view
     * @param account account
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static long balance(final RatisTransaction.ReadView view, final Account account) throws IOException {
        byte[] bytes = view.get(account.shard(), accountKey(account)).orElseThrow(() -> new IllegalArgumentException("account missing"));
        if (bytes.length != Long.BYTES) {
            throw new IOException("invalid balance encoding");
        }
        long value = ByteBuffer.wrap(bytes).getLong();
        if (value < 0) {
            throw new IOException("negative stored balance");
        }
        return value;
    }

    /**
     * Account prefixes and prefixes.
     *
     * @param account account
     * @return the operation result
     */
    private static String accountKey(final Account account) {
        return "account:" + account.id();
    }

    /**
     * The UUID unit does not accept any user keys.
     *
     * @param operation operation
     * @return the operation result
     */
    private static String ledgerKey(final UUID operation) {
        return "transfer:" + operation;
    }

    /**
     * Use a fixed byte order and do not include floating point amounts.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] balanceBytes(final long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    /**
     * The tags include complete requests and original results, and CRC is used to reject accidental damage and does not perform authentication functions.
     *
     * @param applied applied
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static byte[] encode(final Applied applied) throws IOException {
        var buffer = new ByteArrayOutputStream();
        var out = new DataOutputStream(buffer);
        out.writeInt(0x415A5452);
        out.writeInt(1);
        uuid(out, applied.request().operation());
        account(out, applied.request().from());
        account(out, applied.request().to());
        out.writeLong(applied.request().amount());
        uuid(out, applied.transaction());
        out.writeLong(applied.fromBalance());
        out.writeLong(applied.toBalance());
        out.flush();
        if (buffer.size() > 4092) {
            throw new IOException("transfer marker exceeds budget");
        }
        out.writeInt(crc(buffer.toByteArray()));
        out.flush();
        return buffer.toByteArray();
    }

    /**
     * History tags are accepted only after the length, format, validate and business fields have all been passed.
     *
     * @param bytes bytes
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static Applied decode(final byte[] bytes) throws IOException {
        if (bytes.length < 80 || bytes.length > 4096 || ByteBuffer.wrap(bytes).getInt(bytes.length - 4) != crc(Arrays.copyOf(bytes, bytes.length - 4))) {
            throw new IOException("transfer marker checksum invalid");
        }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length - 4));
            if (in.readInt() != 0x415A5452 || in.readInt() != 1) {
                throw new IOException("unsupported transfer marker");
            }
            var request = new Request(uuid(in), account(in), account(in), in.readLong());
            var applied = new Applied(request, uuid(in), in.readLong(), in.readLong());
            if (in.available() != 0) {
                throw new IOException("trailing transfer marker data");
            }
            return applied;
        } catch (final IllegalArgumentException failure) {
            throw new IOException("invalid transfer marker fields", failure);
        }
    }

    /**
     * The small field encoding uses bounded business identifiers.
     *
     * @param out out
     * @param account account
     * @throws IOException if the operation cannot complete
     */
    private static void account(final DataOutputStream out, final Account account) throws IOException {
        out.writeUTF(account.shard());
        out.writeUTF(account.id());
    }

    /**
     * The master record bounded will read fields later, and the constructor continues to check names.
     *
     * @param in in
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static Account account(final DataInputStream in) throws IOException {
        return new Account(in.readUTF(), in.readUTF());
    }

    /**
     * Writes the complete UUID.
     *
     * @param out out
     * @param id id
     * @throws IOException if the operation cannot complete
     */
    private static void uuid(final DataOutputStream out, final UUID id) throws IOException {
        out.writeLong(id.getMostSignificantBits());
        out.writeLong(id.getLeastSignificantBits());
    }

    /**
     * Reads the full UUD.
     *
     * @param in in
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static UUID uuid(final DataInputStream in) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }

    /**
     * The whole markup is common validate.
     *
     * @param bytes bytes
     * @return the operation result
     */
    private static int crc(final byte[] bytes) {
        var crc = new CRC32C();
        crc.update(bytes, 0, bytes.length);
        return (int) crc.getValue();
    }

    /**
     * The account uses a clear shard and restricted key to avoid overlap with the idempotent tag namespace.
     *
     * @param shard logical shard identifier
     * @param id id
     */
    public record Account(String shard, String id) {

        /**
         * Encoding length and account name are before entering transaction.
         *
         * @param shard logical shard identifier
         * @param id id
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public Account {
            Objects.requireNonNull(shard);
            Objects.requireNonNull(id);
            new WalMutation(shard, "", null);
            if (shard.contains("\u0000") || !id.matches("[A-Za-z0-9_-]{1,128}")) {
                throw new IllegalArgumentException("invalid account");
            }
        }
    }

    /**
     * Retry must carry the same single number and complete request, which cannot be changed to another transfer.
     *
     * @param operation operation
     * @param from from
     * @param to to
     * @param amount amount
     */
    public record Request(UUID operation, Account from, Account to, long amount) {

        /**
         * Use the smallest unit of integer currency to refuse negative, zero and self-transfer.
         *
         * @param operation operation
         * @param from from
         * @param to to
         * @param amount amount
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public Request {
            Objects.requireNonNull(operation);
            Objects.requireNonNull(from);
            Objects.requireNonNull(to);
            if (amount <= 0 || from.equals(to)) {
                throw new IllegalArgumentException("invalid transfer");
            }
        }
    }

    /**
     * Returns the balance and original transaction ID when the original transfer is completed, and repeated queries do not replace the current balance.
     *
     * @param request request
     * @param transaction transaction context
     * @param fromBalance from balance
     * @param toBalance to balance
     */
    public record Applied(Request request, UUID transaction, long fromBalance, long toBalance) {

        /**
         * The damaged or unlawful business record cannot be considered to have been applied.
         *
         * @param request request
         * @param transaction transaction context
         * @param fromBalance from balance
         * @param toBalance to balance
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public Applied {
            Objects.requireNonNull(request);
            Objects.requireNonNull(transaction);
            if (fromBalance < 0 || toBalance < 0) {
                throw new IllegalArgumentException("invalid applied balances");
            }
        }
    }

    /**
     * Repeated requests are supported by durable business tags and can be searched for receipt using the original transaction ID separately.
     *
     * @param applied applied
     * @param receipt receipt
     */
    public record Result(Applied applied, Optional<WalReceipt> receipt) {

        /**
         * Defensive empty result, no read-only transaction fake transfer receipt.
         *
         * @param applied applied
         * @param receipt receipt
         */
        public Result {
            Objects.requireNonNull(applied);
            Objects.requireNonNull(receipt);
        }
    }
}
