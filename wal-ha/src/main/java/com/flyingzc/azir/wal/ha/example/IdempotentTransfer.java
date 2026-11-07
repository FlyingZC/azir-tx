package com.flyingzc.azir.wal.ha.example;

import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.wal.WalMutation;
import com.flyingzc.azir.wal.WalReceipt;
import com.flyingzc.azir.wal.ha.RatisManagedNode;
import com.flyingzc.azir.wal.ha.RatisTransaction;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.CRC32C;

/** 跨分片转账示例：业务单号与余额同事务提交，不对外部支付或消息副作用作保证。 */
public final class IdempotentTransfer {
    /** 账户使用明确分片和受限业务键，避免与幂等标记命名空间重叠。 */
    public record Account(String shard, String id) {
        /** 编码长度和账户名称均在进入事务前校验。 */
        public Account { Objects.requireNonNull(shard); Objects.requireNonNull(id); new WalMutation(shard, "", null); if (shard.contains("\u0000") || !id.matches("[A-Za-z0-9_-]{1,128}")) { throw new IllegalArgumentException("invalid account"); } }
    }
    /** 重试必须携带相同单号及完整请求，不能把同一单号改成另一笔转账。 */
    public record Request(UUID operation, Account from, Account to, long amount) {
        /** 使用整数最小货币单位，拒绝负数、零额及自己转自己。 */
        public Request { Objects.requireNonNull(operation); Objects.requireNonNull(from); Objects.requireNonNull(to); if (amount <= 0 || from.equals(to)) { throw new IllegalArgumentException("invalid transfer"); } }
    }
    /** 返回原始转账完成时的余额及原事务号，重复查询不替换为当前余额。 */
    public record Applied(Request request, UUID transaction, long fromBalance, long toBalance) {
        /** 损坏或不合法的业务记录不能当作已应用。 */
        public Applied { Objects.requireNonNull(request); Objects.requireNonNull(transaction); if (fromBalance < 0 || toBalance < 0) { throw new IllegalArgumentException("invalid applied balances"); } }
    }
    /** 重复请求由持久业务标记证明已应用，可单独用原事务号查询凭据。 */
    public record Result(Applied applied, Optional<WalReceipt> receipt) {
        /** 防御空结果，不用只读事务的新号冒充原转账凭据。 */
        public Result { Objects.requireNonNull(applied); Objects.requireNonNull(receipt); }
    }
    private final RatisManagedNode node;
    private final String ledgerShard;
    /** 调用方负责在所有节点配置相同路由，标记与账户数据均必须经过本模式访问。 */
    public IdempotentTransfer(RatisManagedNode node, String ledgerShard) { this.node = Objects.requireNonNull(node); new WalMutation(ledgerShard, "", null); if (ledgerShard.contains("\u0000")) { throw new IllegalArgumentException("invalid ledger shard"); } this.ledgerShard = ledgerShard; }

    /** 初始化仅允许全新账户，同一事务内任一账户已存在则全部不写。 */
    public WalReceipt initializeAccounts(Map<Account, Long> balances) throws Exception {
        var values = Map.copyOf(balances); if (values.isEmpty() || values.values().stream().anyMatch(value -> value < 0)) { throw new IllegalArgumentException("invalid initial balances"); }
        return node.execute(IsolationLevel.REPEATABLE_READ, transaction -> {
            var view = transaction.statement();
            for (var account : values.keySet()) { if (view.get(account.shard(), accountKey(account)).isPresent()) { throw new IllegalStateException("account already exists"); } }
            for (var entry : values.entrySet()) { transaction.put(entry.getKey().shard(), accountKey(entry.getKey()), balanceBytes(entry.getValue())); }
            return null;
        }).receipt();
    }
    /** 没有自动重试：明确冲突可由调用方重提同一业务单号，未知结果也不可换新单号。 */
    public Result transfer(Request request) throws Exception {
        Objects.requireNonNull(request);
        var execution = node.execute(IsolationLevel.REPEATABLE_READ, transaction -> {
            var view = transaction.statement(); var prior = view.get(ledgerShard, ledgerKey(request.operation()));
            if (prior.isPresent()) {
                var applied = decode(prior.get());
                if (!applied.request().equals(request)) { throw new IllegalArgumentException("operation ID already used for a different request"); }
                return applied;
            }
            long from = balance(view, request.from()), to = balance(view, request.to());
            if (from < request.amount()) { throw new IllegalArgumentException("insufficient balance"); }
            var applied = new Applied(request, transaction.id(), from - request.amount(), Math.addExact(to, request.amount()));
            transaction.put(request.from().shard(), accountKey(request.from()), balanceBytes(applied.fromBalance()));
            transaction.put(request.to().shard(), accountKey(request.to()), balanceBytes(applied.toBalance()));
            transaction.put(ledgerShard, ledgerKey(request.operation()), encode(applied));
            return applied;
        });
        return new Result(execution.value(), execution.durableDecision() ? Optional.of(execution.receipt()) : Optional.empty());
    }
    /** 查询业务标记；缺失仅表示当前快照没看到，不允许据此断言原未知事务已回滚。 */
    public Optional<Applied> find(UUID operation) throws Exception {
        Objects.requireNonNull(operation);
        return node.execute(IsolationLevel.REPEATABLE_READ, transaction -> {
            var bytes = transaction.get(ledgerShard, ledgerKey(operation));
            if (bytes.isEmpty()) { return Optional.<Applied>empty(); }
            var value = decode(bytes.get()); if (!value.request().operation().equals(operation)) { throw new IOException("transfer marker key mismatch"); } return Optional.of(value);
        }).value();
    }
    /** 样例点读；审计多个账户时应在同一事务同一 ReadView 中读取。 */
    public long balance(Account account) throws Exception { return node.execute(IsolationLevel.REPEATABLE_READ, transaction -> balance(transaction.statement(), account)).value(); }
    /** 严格读取固定八字节非负余额，缺失账户不能隐式按零处理。 */
    private static long balance(RatisTransaction.ReadView view, Account account) throws IOException {
        byte[] bytes = view.get(account.shard(), accountKey(account)).orElseThrow(() -> new IllegalArgumentException("account missing"));
        if (bytes.length != Long.BYTES) { throw new IOException("invalid balance encoding"); } long value = ByteBuffer.wrap(bytes).getLong(); if (value < 0) { throw new IOException("negative stored balance"); } return value;
    }
    /** 账户键前缀与标记键前缀互斥。 */
    private static String accountKey(Account account) { return "account:" + account.id(); }
    /** UUID 单号不会接受任意用户键。 */
    private static String ledgerKey(UUID operation) { return "transfer:" + operation; }
    /** 使用固定字节序，不引入浮点金额。 */
    private static byte[] balanceBytes(long value) { return ByteBuffer.allocate(Long.BYTES).putLong(value).array(); }
    /** 标记包括完整请求和原结果，CRC 用于拒绝意外损坏，不承担认证功能。 */
    private static byte[] encode(Applied applied) throws IOException {
        var buffer = new ByteArrayOutputStream(); var out = new DataOutputStream(buffer); out.writeInt(0x415A5452); out.writeInt(1);
        uuid(out, applied.request().operation()); account(out, applied.request().from()); account(out, applied.request().to()); out.writeLong(applied.request().amount()); uuid(out, applied.transaction()); out.writeLong(applied.fromBalance()); out.writeLong(applied.toBalance()); out.flush();
        if (buffer.size() > 4092) { throw new IOException("transfer marker exceeds budget"); } out.writeInt(crc(buffer.toByteArray())); out.flush(); return buffer.toByteArray();
    }
    /** 长度、格式、校验与业务字段全部通过以后才接受历史标记。 */
    private static Applied decode(byte[] bytes) throws IOException {
        if (bytes.length < 80 || bytes.length > 4096 || ByteBuffer.wrap(bytes).getInt(bytes.length - 4) != crc(Arrays.copyOf(bytes, bytes.length - 4))) { throw new IOException("transfer marker checksum invalid"); }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length - 4)); if (in.readInt() != 0x415A5452 || in.readInt() != 1) { throw new IOException("unsupported transfer marker"); }
            var request = new Request(uuid(in), account(in), account(in), in.readLong()); var applied = new Applied(request, uuid(in), in.readLong(), in.readLong()); if (in.available() != 0) { throw new IOException("trailing transfer marker data"); } return applied;
        } catch (IllegalArgumentException failure) { throw new IOException("invalid transfer marker fields", failure); }
    }
    /** 小字段编码使用有界业务标识。 */
    private static void account(DataOutputStream out, Account account) throws IOException { out.writeUTF(account.shard()); out.writeUTF(account.id()); }
    /** 总记录有界以后再读取字段，构造器继续检查名称。 */
    private static Account account(DataInputStream in) throws IOException { return new Account(in.readUTF(), in.readUTF()); }
    /** 写入完整 UUID。 */
    private static void uuid(DataOutputStream out, UUID id) throws IOException { out.writeLong(id.getMostSignificantBits()); out.writeLong(id.getLeastSignificantBits()); }
    /** 读取完整 UUID。 */
    private static UUID uuid(DataInputStream in) throws IOException { return new UUID(in.readLong(), in.readLong()); }
    /** 整份标记共同校验。 */
    private static int crc(byte[] bytes) { var crc = new CRC32C(); crc.update(bytes, 0, bytes.length); return (int) crc.getValue(); }
}
