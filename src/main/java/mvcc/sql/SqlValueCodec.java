package mvcc.sql;

import java.nio.charset.StandardCharsets;

/**
 * SQL 字符串值和 MVCC 字节值之间的编解码接口。
 */
public interface SqlValueCodec {
    /**
     * 把 SQL 字符串值编码为存储字节。
     */
    byte[] encode(String value);

    /**
     * 把存储字节解码为 SQL 字符串值。
     */
    String decode(byte[] value);

    /**
     * 返回 UTF-8 字符串编解码实现。
     */
    static SqlValueCodec utf8() {
        return Utf8SqlValueCodec.INSTANCE;
    }

    /**
     * UTF-8 字符串编解码实现。
     */
    final class Utf8SqlValueCodec implements SqlValueCodec {
        private static final Utf8SqlValueCodec INSTANCE = new Utf8SqlValueCodec();

        /**
         * 构造 UTF-8 编解码实现。
         */
        private Utf8SqlValueCodec() {
        }

        /**
         * 把字符串编码为 UTF-8 字节。
         */
        @Override
        public byte[] encode(String value) {
            return value.getBytes(StandardCharsets.UTF_8);
        }

        /**
         * 把 UTF-8 字节解码为字符串。
         */
        @Override
        public String decode(byte[] value) {
            return new String(value, StandardCharsets.UTF_8);
        }
    }
}
