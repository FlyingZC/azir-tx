package com.flyingzc.azir.storage.mysql;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 只读切面的有界二进制网络格式，代数、路由和每条序号一并校验。 */
final class MysqlVectorCutWire {
    private static final int MAGIC = 0x415A5643;
    private static final int VERSION = 1;
    private static final int MAX_BYTES = 1024 * 1024;

    /** 网络编解码器无需保存进程状态。 */
    private MysqlVectorCutWire() {
    }

    /** 将领导者代数和固定切面编码为一个不可拆分的响应体。 */
    static byte[] encode(MysqlVectorCutLeaderRuntime.PublishedView view) {
        try (var bytes = new ByteArrayOutputStream(); var output = new DataOutputStream(bytes)) {
            output.writeInt(MAGIC);
            output.writeInt(VERSION);
            output.writeLong(view.generation());
            output.writeLong(view.snapshot().decisionEpoch());
            List<String> routes = view.snapshot().localSequences().keySet().stream().sorted().toList();
            output.writeInt(routes.size());
            for (String route : routes) {
                byte[] encodedName = route.getBytes(StandardCharsets.UTF_8);
                output.writeInt(encodedName.length);
                output.write(encodedName);
                List<Long> sequences = view.snapshot().localSequences().get(route);
                output.writeInt(sequences.size());
                for (long sequence : sequences) {
                    output.writeLong(sequence);
                }
            }
            output.flush();
            byte[] encoded = bytes.toByteArray();
            if (encoded.length > MAX_BYTES) {
                throw new IllegalArgumentException("vector cut response exceeds wire limit");
            }
            return encoded;
        } catch (IOException ex) {
            throw new IllegalStateException("encode in-memory vector cut failed", ex);
        }
    }

    /** 客户端拒绝错误版本、代数、路由布局、分条数量及截断响应。 */
    static MysqlVectorCutLeaderRuntime.PublishedView decode(byte[] encoded,
                                                              List<String> expectedRoutes, int stripes) {
        if (encoded == null || encoded.length < 28 || encoded.length > MAX_BYTES
                || expectedRoutes == null || expectedRoutes.isEmpty()
                || expectedRoutes.size() > 128 || stripes < 1 || stripes > 256) {
            throw new IllegalArgumentException("invalid vector cut wire length or layout");
        }
        try (var input = new DataInputStream(new ByteArrayInputStream(encoded))) {
            if (input.readInt() != MAGIC || input.readInt() != VERSION) {
                throw new IllegalArgumentException("unknown vector cut wire protocol");
            }
            long generation = input.readLong();
            long epoch = input.readLong();
            int routeCount = input.readInt();
            if (generation < 1 || epoch < 0 || routeCount != expectedRoutes.size()) {
                throw new IllegalArgumentException("vector cut wire generation or route count changed");
            }
            Map<String, List<Long>> values = new LinkedHashMap<>();
            for (int index = 0; index < routeCount; index++) {
                int nameLength = input.readInt();
                if (nameLength < 1 || nameLength > 128 || nameLength > input.available()) {
                    throw new IllegalArgumentException("vector cut wire route name is invalid");
                }
                byte[] routeBytes = input.readNBytes(nameLength);
                String route = new String(routeBytes, StandardCharsets.UTF_8);
                if (!expectedRoutes.get(index).equals(route) || input.readInt() != stripes) {
                    throw new IllegalArgumentException("vector cut wire route layout changed");
                }
                Long[] sequences = new Long[stripes];
                for (int stripe = 0; stripe < stripes; stripe++) {
                    long sequence = input.readLong();
                    if (sequence < 0) {
                        throw new IllegalArgumentException("vector cut wire has negative sequence");
                    }
                    sequences[stripe] = sequence;
                }
                values.put(route, List.of(sequences));
            }
            if (input.available() != 0) {
                throw new IllegalArgumentException("vector cut wire contains trailing bytes");
            }
            return new MysqlVectorCutLeaderRuntime.PublishedView(generation,
                    new MysqlStripedSnapshotOracle.Snapshot(epoch, values));
        } catch (IOException ex) {
            throw new IllegalArgumentException("vector cut wire response was truncated", ex);
        }
    }
}
