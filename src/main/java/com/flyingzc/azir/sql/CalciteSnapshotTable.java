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

package com.flyingzc.azir.sql;

import com.flyingzc.azir.api.Transaction;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Utf8KeyOrder;
import com.flyingzc.azir.txn.StatementReadSnapshot;
import com.flyingzc.azir.txn.TransactionManager;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The same sentence snapshot is exposed to a page break table where the main key condition can be pushed down, and only reads storage as the ticker moves.
 */
final class CalciteSnapshotTable extends AbstractTable implements ProjectableFilterableTable {

    private final SqlReadPort reads;

    private final String table;

    private final ShardResolver shardResolver;

    private final SqlStorageKeyCodec keyCodec;

    private final SqlValueCodec valueCodec;

    private final int pageSize;

    private final SqlScanBudget scanBudget;

    private final int rowLimit;

    /**
     * Ties statements snapshot and decodors without access to shard at the construction stage.
     *
     * @param transactionManager transaction manager
     * @param tx transaction context
     * @param snapshot snapshot
     * @param table table
     * @param shardResolver shard resolver
     * @param keyCodec key codec
     * @param valueCodec value codec
     * @param pageSize page size
     * @param scanBudget scan budget
     */
    CalciteSnapshotTable(final TransactionManager transactionManager, final Transaction tx, final StatementReadSnapshot snapshot,
                         final String table, final ShardResolver shardResolver, final SqlStorageKeyCodec keyCodec,
                         final SqlValueCodec valueCodec, final int pageSize, final SqlScanBudget scanBudget) {
        this(transactionManager, tx, snapshot, table, shardResolver, keyCodec, valueCodec,
                pageSize, scanBudget, Integer.MAX_VALUE);
    }

    /**
     * Sets a safe line-scan limit for single table LIMIT queries that do not require sorting or filtering.
     *
     * @param transactionManager transaction manager
     * @param tx transaction context
     * @param snapshot snapshot
     * @param table table
     * @param shardResolver shard resolver
     * @param keyCodec key codec
     * @param valueCodec value codec
     * @param pageSize page size
     * @param scanBudget scan budget
     * @param rowLimit row limit
     */
    CalciteSnapshotTable(final TransactionManager transactionManager, final Transaction tx, final StatementReadSnapshot snapshot,
                         final String table, final ShardResolver shardResolver, final SqlStorageKeyCodec keyCodec,
                         final SqlValueCodec valueCodec, final int pageSize, final SqlScanBudget scanBudget, final int rowLimit) {
        this(new MysqlSqlReadPort(transactionManager, tx, snapshot), table,
                shardResolver, keyCodec, valueCodec, pageSize, scanBudget, rowLimit);
    }

    /**
     * Native transaction provides a fixed speech reading port, Calcite does not need to parse the backend commit time.
     *
     * @param reads reads
     * @param table table
     * @param shardResolver shard resolver
     * @param keyCodec key codec
     * @param valueCodec value codec
     * @param pageSize page size
     * @param scanBudget scan budget
     * @param rowLimit row limit
     */
    CalciteSnapshotTable(final SqlReadPort reads, final String table, final ShardResolver shardResolver,
                         final SqlStorageKeyCodec keyCodec, final SqlValueCodec valueCodec,
                         final int pageSize, final SqlScanBudget scanBudget, final int rowLimit) {
        this.reads = java.util.Objects.requireNonNull(reads, "reads");
        this.table = table;
        this.shardResolver = shardResolver;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
        this.pageSize = pageSize;
        this.scanBudget = scanBudget;
        this.rowLimit = rowLimit;
    }

    /**
     * Maintains the original non-id/value string list model.
     *
     * @param typeFactory type factory
     * @return the operation result
     */
    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
        return typeFactory.builder().add("id", SqlTypeName.VARCHAR).add("value", SqlTypeName.VARCHAR).build();
    }

    /**
     * Select a primary key condition that you can accurately execute, and the remaining condition is calculated by Calcite.
     *
     * @param root root
     * @param filters filters
     * @param projects projects
     * @return the operation result
     */
    @Override
    public Enumerable<Object[]> scan(final DataContext root, final List<RexNode> filters, final int[] projects) {
        int scanRowLimit = filters.isEmpty() ? rowLimit : Integer.MAX_VALUE;
        KeyRestriction restriction = selectRestriction(filters);
        int[] projection = projects == null ? new int[]{0, 1} : projects.clone();
        AtomicBoolean cancelled = DataContext.Variable.CANCEL_FLAG.get(root);
        return new AbstractEnumerable<>() {

            /**
             * Each count creates an independent cursor, JOIN repeats the original statement snapshot.
             *
             * @return the operation result
             */
            @Override
            public Enumerator<Object[]> enumerator() {
                return new PageEnumerator(restriction, projection, cancelled, scanRowLimit);
            }
        };
    }

    /**
     * Priority is given to an equivalent check, otherwise the first security range is used; not merge or rewriting unaccepted expressions.
     *
     * @param filters filters
     * @return the operation result
     */
    private KeyRestriction selectRestriction(final List<RexNode> filters) {
        KeyRestriction selected = null;
        RexNode accepted = null;
        for (RexNode filter : filters) {
            KeyRestriction candidate = decodeRestriction(filter);
            if (candidate != null && (selected == null || candidate.pointKey() != null)) {
                selected = candidate;
                accepted = filter;
                if (candidate.pointKey() != null) {
                    break;
                }
            }
        }
        if (accepted != null) {
            filters.remove(accepted);
        }
        return selected == null ? new KeyRestriction(null, keyCodec.rangeStart(table), keyCodec.rangeEnd(table)) : selected;
    }

    /**
     * Decoding only the direct primary key reference to compares with a string constant and rejects the condition that NULL, CAST, values and sorting rules are uncertain.
     *
     * @param filter filter
     * @return the operation result
     */
    private KeyRestriction decodeRestriction(final RexNode filter) {
        if (!(filter instanceof RexCall call) || call.getOperands().size() != 2) {
            return null;
        }
        SqlKind kind = call.getKind();
        if (kind != SqlKind.EQUALS && kind != SqlKind.GREATER_THAN
                && kind != SqlKind.GREATER_THAN_OR_EQUAL && kind != SqlKind.LESS_THAN
                && kind != SqlKind.LESS_THAN_OR_EQUAL) {
            // In conditions such as IN may also use RexLiteral packaging SARG, which cannot be decoded by string.
            return null;
        }
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);
        if (left instanceof RexLiteral && right instanceof RexInputRef) {
            RexNode swapped = left;
            left = right;
            right = swapped;
            kind = kind.reverse();
        }
        if (!(left instanceof RexInputRef ref) || ref.getIndex() != 0
                || !(right instanceof RexLiteral literal) || literal.isNull()
                || left.getType().getSqlTypeName() != SqlTypeName.VARCHAR
                || (right.getType().getSqlTypeName() != SqlTypeName.CHAR
                        && right.getType().getSqlTypeName() != SqlTypeName.VARCHAR)
                || !usesStringOrder(left.getType()) || !usesStringOrder(right.getType())) {
            return null;
        }
        String id = literal.getValueAs(String.class);
        if (id == null || !StandardCharsets.UTF_8.newEncoder().canEncode(id)) {
            return null;
        }
        if (kind == SqlKind.EQUALS) {
            return new KeyRestriction(id, null, null);
        }
        // ASCII Boundaries are in the same order as String.compareTo when comparing arbitrary and legal UTF-8 keys; non-ASCII Boundary is handed over to Calcite.
        if (!id.chars().allMatch(character -> character < 128)) {
            return null;
        }
        String key = keyCodec.encode(table, id);
        return switch (kind) {
            case GREATER_THAN -> new KeyRestriction(null, key + '\u0000', keyCodec.rangeEnd(table));
            case GREATER_THAN_OR_EQUAL -> new KeyRestriction(null, key, keyCodec.rangeEnd(table));
            case LESS_THAN -> new KeyRestriction(null, keyCodec.rangeStart(table), key);
            case LESS_THAN_OR_EQUAL -> new KeyRestriction(null, keyCodec.rangeStart(table), key + '\u0000');
            default -> null;
        };
    }

    /**
     * Maps to binary storage keys are allowed only if a Calcite string comparison does not require Collator.
     *
     * @param type type
     * @return the operation result
     */
    private boolean usesStringOrder(final RelDataType type) {
        return type.getCollation() != null && type.getCollation().getCollator() == null;
    }

    /**
     * Describes the physical scanning range of a single key or left-to-right closed, and click keys to keep the logical primary key for route use.
     *
     * @param pointKey point key
     * @param startKey start key
     * @param endKey end key
     */
    private record KeyRestriction(String pointKey, String startKey, String endKey) {
    }

    /**
     * A single-page viewer will not request a page after the original snapshot, LIMIT has been closed.
     */
    private final class PageEnumerator implements Enumerator<Object[]> {

        private final KeyRestriction restriction;

        private final int[] projects;

        private final AtomicBoolean cancelled;

        private final int scanRowLimit;

        private final List<String> shards;

        private List<SqlReadPort.KeyValue> page = List.of();

        private int pageIndex;

        private int shardIndex;

        private String afterKey;

        private Object[] current;

        private boolean pointRead;

        private boolean closed;

        private int emittedRows;

        /**
         * Create an item count state; check-in only the single shard and range scan covers all shard routers declared.
         *
         * @param restriction restriction
         * @param projects projects
         * @param cancelled cancelled
         * @param scanRowLimit scan row limit
         */
        private PageEnumerator(final KeyRestriction restriction, final int[] projects, final AtomicBoolean cancelled, final int scanRowLimit) {
            this.restriction = restriction;
            this.projects = projects;
            this.cancelled = cancelled;
            this.scanRowLimit = scanRowLimit;
            this.shards = restriction.pointKey() == null
                    ? shardResolver.resolveShards(table).stream().distinct().toList()
                    : List.of(shardResolver.resolveShard(table, restriction.pointKey()));
        }

        /**
         * Returns the current projection, does not locate or is closed.
         *
         * @return the operation result
         * @throws NoSuchElementException if the operation cannot complete
         */
        @Override
        public Object[] current() {
            if (current == null) {
                throw new NoSuchElementException("no current SQL row");
            }
            return current;
        }

        /**
         * The empty visible pages continue to be read in line with the original scan cursor as consumption demands advance.
         *
         * @return the operation result
         * @throws MvccException if the operation cannot complete
         */
        @Override
        public boolean moveNext() {
            current = null;
            if (closed) {
                return false;
            }
            if (emittedRows >= scanRowLimit) {
                return false;
            }
            try {
                scanBudget.ensureAvailable();
                if (cancelled != null && cancelled.get()) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE, "SQL query cancelled", false,
                            reads.diagnosticTransactionId());
                }
                if (restriction.pointKey() != null) {
                    if (!pointRead) {
                        pointRead = true;
                        String physicalKey = keyCodec.encode(table, restriction.pointKey());
                        var value = reads.get(shards.getFirst(), physicalKey);
                        if (value.isPresent()) {
                            scanBudget.chargeRow(physicalKey, value.get());
                            current = project(restriction.pointKey(), value.get());
                            emittedRows++;
                            return true;
                        }
                    }
                    return false;
                }
                while (true) {
                    if (cancelled != null && cancelled.get()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE, "SQL query cancelled", false,
                                reads.diagnosticTransactionId());
                    }
                    if (pageIndex < page.size()) {
                        SqlReadPort.KeyValue value = page.get(pageIndex++);
                        if (!value.deleted()) {
                            current = project(keyCodec.decode(table, value.key()), value.value());
                            emittedRows++;
                            return true;
                        }
                    } else if (shardIndex < shards.size()) {
                        fetchPage();
                    } else {
                        page = List.of();
                        return false;
                    }
                }
            } catch (final RuntimeException | Error ex) {
                close();
                throw ex;
            }
        }

        /**
         * Ask for the next page after releasing the old page and check cursor forward in UTF-8 unsigned bytes.
         *
         * @throws MvccException if the operation cannot complete
         */
        private void fetchPage() {
            page = List.of();
            pageIndex = 0;
            SqlReadPort.Page fetched = reads.scanPage(shards.get(shardIndex),
                    new KeyRange(restriction.startKey(), restriction.endKey()), afterKey, pageSize);
            String next = fetched.nextKey();
            if (next != null && afterKey != null && Utf8KeyOrder.compare(next, afterKey) <= 0) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "SQL scan cursor did not advance", false,
                        reads.diagnosticTransactionId());
            }
            // The entire page is billed before being handed over to Calcite, and the unconsumpted LIMIT prep line and residual filter abandoned lines also occupy input budget.
            for (SqlReadPort.KeyValue value : fetched.values()) {
                scanBudget.chargeRow(value.key(), value.value());
            }
            page = fetched.values();
            afterKey = next;
            if (next == null) {
                shardIndex++;
            }
        }

        /**
         * Decoding only the value required to project and avoiding primary-key queries with COUNT to assign value strings.
         *
         * @param id id
         * @param value value to write
         * @return the operation result
         * @throws IllegalArgumentException if the operation cannot complete
         */
        private Object[] project(final String id, final byte[] value) {
            Object[] result = new Object[projects.length];
            String decodedValue = null;
            for (int index = 0; index < projects.length; index++) {
                if (projects[index] == 0) {
                    result[index] = id;
                } else if (projects[index] == 1) {
                    if (decodedValue == null) {
                        decodedValue = valueCodec.decode(value);
                    }
                    result[index] = decodedValue;
                } else {
                    throw new IllegalArgumentException("unknown SQL column: " + projects[index]);
                }
            }
            return result;
        }

        /**
         * Reset cursor for relationships that need to be cleaned; use original statement snapshot and do not access storage when reset.
         *
         * @throws IllegalStateException if the operation cannot complete
         */
        @Override
        public void reset() {
            if (closed) {
                throw new IllegalStateException("SQL scan enumerator is closed");
            }
            page = List.of();
            pageIndex = 0;
            shardIndex = 0;
            afterKey = null;
            current = null;
            pointRead = false;
            emittedRows = 0;
        }

        /**
         * Release idempotently Current page with current line; each page call has closed the bottom storage resource from the transaction layer.
         */
        @Override
        public void close() {
            closed = true;
            page = List.of();
            current = null;
        }
    }
}
