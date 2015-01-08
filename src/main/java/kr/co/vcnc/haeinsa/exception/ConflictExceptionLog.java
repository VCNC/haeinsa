package kr.co.vcnc.haeinsa.exception;

import kr.co.vcnc.haeinsa.HaeinsaRowTransaction;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Objects;

public class ConflictExceptionLog {
    private byte[] row;
    private TRowLock oldRowLock;
    private TRowLock newRowLock;

    public static ConflictExceptionLog from(byte[] row, HaeinsaRowTransaction rowTx) {
        ConflictExceptionLog log = new ConflictExceptionLog();
        log.row = row;
        log.oldRowLock = rowTx.getCurrent();
        return log;
    }

    public static ConflictExceptionLog from(byte[] row, HaeinsaRowTransaction rowTx, TRowLock newRowLock) {
        ConflictExceptionLog log = from(row, rowTx);
        log.newRowLock = newRowLock;
        return log;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("row", Bytes.toStringBinary(row))
                .add("oldRowLock", oldRowLock)
                .add("newRowLock", newRowLock)
                .toString();
    };
}
