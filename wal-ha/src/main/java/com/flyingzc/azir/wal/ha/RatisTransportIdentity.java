package com.flyingzc.azir.wal.ha;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.zip.CRC32C;
import com.flyingzc.azir.wal.WalReplicaIdentity;

/** 持久记录认证模式，重启不能无意把同一副本降级为明文服务。 */
final class RatisTransportIdentity {
    /** 工具类不允许创建实例。 */
    private RatisTransportIdentity() { }
    /** 启动前只核对，不在尚未取得业务根目录锁时写入文件。 */
    static void verify(Path directory, WalReplicaIdentity identity, boolean authenticated) throws IOException {
        Path marker = directory.resolve("AUTHENTICATED"); if (!Files.exists(marker, LinkOption.NOFOLLOW_LINKS)) { return; }
        if (!authenticated || !Files.isRegularFile(marker, LinkOption.NOFOLLOW_LINKS) || Files.size(marker) > 256) { throw new IOException("authenticated replica cannot downgrade transport"); }
        byte[] actual = Files.readAllBytes(marker); if (!Arrays.equals(actual, encode(identity))) { throw new IOException("authenticated transport identity mismatch"); }
    }
    /** 取得业务根目录锁后持久发布，已有正确标记也重新同步，启动失败不能绕过禁止降级。 */
    static void persist(Path directory, WalReplicaIdentity identity) throws IOException { persist(directory, identity, RatisPolicyFile.REAL); }
    /** 包内 I/O 注入与正式认证标记共用真实同步及重命名协议。 */
    static void persist(Path directory, WalReplicaIdentity identity, RatisPolicyFile.Io io) throws IOException {
        verify(directory, identity, true); RatisPolicyFile.publish(directory, RatisPolicyFile.Kind.AUTHENTICATED, encode(identity), io);
    }
    /** 固定编码包含完整组和节点身份，CRC 同时保护认证模式版本。 */
    private static byte[] encode(WalReplicaIdentity identity) throws IOException {
        var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes); out.writeInt(0x415A544C); out.writeInt(1); out.writeUTF(identity.engineId().toString()); out.writeUTF(identity.groupId().toString()); out.writeUTF(identity.nodeId()); out.flush(); var crc = new CRC32C(); crc.update(bytes.toByteArray()); out.writeInt((int) crc.getValue()); out.flush(); return bytes.toByteArray();
    }
}
