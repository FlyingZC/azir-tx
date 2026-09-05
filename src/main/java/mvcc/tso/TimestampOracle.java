package mvcc.tso;

public interface TimestampOracle {
    long nextTimestamp();
}
