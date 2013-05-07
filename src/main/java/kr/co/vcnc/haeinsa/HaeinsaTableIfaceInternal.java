package kr.co.vcnc.haeinsa;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;

import kr.co.vcnc.haeinsa.exception.ConflictException;
import kr.co.vcnc.haeinsa.thrift.generated.TMutationType;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLockState;

/**
 * {@link HaeinsaTableIface}의 인터페이스외에 내부적으로 공유되어야할 인터페이스들을 모아둔 인터페이스
 */
interface HaeinsaTableIfaceInternal extends HaeinsaTableIface {

	/**
	 * Commit single row put only Transaction.
	 * Directly change {@link TRowLockState} from {@link TRowLockState#STABLE} to {@link TRowLockState#STABLE} and
	 * increase commitTimestamp by 1.
	 * Separate this because Single Row put only transaction can save one checkAndPut operation to complete.
	 *
	 * <p>If TRowLock is changed and checkAndPut failed, it means transaction is failed so throw {@link ConflictException}.
	 * @param rowState
	 * @param row
	 * @throws IOException ConflictException, HBase IOException.
	 */
	void commitSingleRowPutOnly(HaeinsaRowTransaction rowState, byte[] row) throws IOException;

	/**
	 * Commit single row read only Transaction.
	 * Read {@link TRowLock} from HBase and compare that lock with saved one which have retrieved when start transaction.
	 * If TRowLock is changed, it means transaction is failed, so throw {@link ConflictException}.
	 * @param rowState
	 * @param row
	 * @throws IOException  ConflictException, HBase IOException.
	 */
	void commitSingleRowReadOnly(HaeinsaRowTransaction rowState, byte[] row) throws IOException;

	/**
	 * Read {@link TRowLock} from HBase and compare that lock with prevRowLock.
	 * If TRowLock is changed, it means transaction is failed, so throw {@link ConflictException}.
	 * @param prevRowLock
	 * @param row
	 * @throws IOException ConflictException, HBase IOException.
	 * @throws NullPointException if oldLock is null (haven't read lock from HBase)
	 */
	void checkSingleRowLock(HaeinsaRowTransaction rowState, byte[] row) throws IOException;

	/**
	 * rowState 값을 참조하여 row 에 prewrite 를 한다.
	 * {@link TRowLock} 의 version, state, commitTimestamp, currentTimestamp 를 기록한다.
	 * rowState 의 첫 번째 mutation 이 HaeinsaPut 일 경우 그 put 들도 모아서 lock 을 {@link TRowLockState#PREWRITTEN} 으로 변경할 때
	 * 함께 변경한다.
	 * mutation 들 중에서 아직 적용 되지 않은 mutation 들은 {@link TRowLock#mutations} 에 기록하고,
	 * prewrite 동안에 put 된 데이터는 {@link TRowLock#prewritten} 에 기록된다.
	 * 후자는 후에 {@link HaeinsaTransaction#abort()} 에서 기록된 데이터를 제거할 때 사용된다.
	 * <p> primary row 일 경우에는 secondaries 가 추가되고, secondary row 일 경우에는 primary 가 추가된다.
	 * @param rowState
	 * @param row
	 * @param isPrimary
	 * @throws IOException ConflictException, HBase IOException
	 */
	void prewrite(HaeinsaRowTransaction rowState, byte[] row, boolean isPrimary) throws IOException;

	/**
	 * {@link TRowLockState#PREWRITTEN} state 에서 Row 의 lock 에 적혀 있는 mutation 들을 모두 적용해 주는 method 이다.
	 * <p> REMOVE 와 PUT 형태의 mutation 을 번갈아 가면서 적용하게 되며, PUT mutation 을 적용할 때만 HBase 에 적힌 TRowLock 이 최신 값으로 변경된다.
	 * <p> mutation 의 종류가 {@link TMutationType#REMOVE} 일 경우에는 증가된 currentTimestamp 와 줄어든 remaining 을
	 * {@link TRowLock} 에 기록하지 못한다.
	 * 따라서 REMOVE mutation 에 실패한 후에 transaction 이 중단되는 경우, 다른 클라이언트가 받아서 recover 를 시도하면
	 * currentTimestamp 가 1만큼 작을 수 있다.
	 * 이 경우에도 단지 이미 delete 명령이 수행된 timestamp 에 해당 명령이 한 번 더 수행될 뿐 정상적인 transaction 종료에 문제는 없다.
	 *
	 * @param rowTxState
	 * @param row
	 * @throws IOException ConflictException, HBase IOException.
	 */
	void applyMutations(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException;

	/**
	 * 특정 row 의 {@link TRowLock} 를 Stable 로 바꾼다.
	 * HBase 의 timestamp 가 commitTimestamp 인 곳에 쓰여진다.
	 * {@link TRowLock#version}, {@link TRowLock#state} 와 {@link TRowLock#commitTimestamp} 정보만 쓰여진다.
	 * @param tx
	 * @param row
	 * @throws IOException ConflictException, HBase IOException.
	 */
	void makeStable(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException;

	/**
	 * 특정 primary row 의 state 를 {@link TRowLockState#COMMITTED} 로 바꾼다.
	 * Transaction 이 정상적으로 수행되는 동안 해당 method 가 불릴 경우
	 * state 가 {@link TRowLockState#PREWRITTEN} 에서 {@link TRowLockState#COMMITTED} 로 바뀌게 되며,
	 * Transaction 이 stable 상태까지 도달하는 데 실패하고 {@link TRowLockState#COMMITTED} 에서 중단된 경우
	 * {@link HaeinsaTransaction} 에 의해서 불려서 primary row 의 state 를
	 * {@link TRowLockState#COMMITTED} 를 {@link TRowLockState#COMMITTED} 로 바꾼다.
	 *
	 * @param tx
	 * @param row
	 * @throws IOException ConflictException, HBase IOException.
	 */
	 void commitPrimary(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException;

	/**
	 * get {@link TRowLock} from HBase.
	 *
	 * @param row row
	 * @return row lock
	 * @throws IOException HBase IOException.
	 */
	TRowLock getRowLock(byte[] row) throws IOException;

	/**
	 * 중단되거나 lock 을 획득하는 데 실패한 Transaction 을 Transaction 을 시도하기 전의 상태로 rollback 하기 위해서
	 * primary row 의 lock 를 {@link TRowLockState#ABORTED} 로 바꾸는 method 이다.
	 * <p>primary row 의 lock 은 {@link TRowLockState#PREWRITTEN} 상태에서 {@link TRowLockState#ABORTED} 로 바뀔 수도 있고,
	 * 하나의 {@link TRowLockState#ABORTED} state 를 가진 lock 에서 또 다른 {@link TRowLockState#ABORTED} 상태의 lock 으로 바뀔 수도 있다.
	 * 후자는 다른 클라이언트가 해당 row 의 실패한 transaction 을 abort 하는 시도를 하다가 다시 실패한 경우에 해당한다.
	 * @param tx
	 * @param row
	 * @throws IOException ConflictException, HBase IOException.
	 */
	void abortPrimary(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException;

	/**
	 * {@link TRowLockState#PREWRITTEN} 과정에서 해당 row 에 put 된 데이터를 지우는 역할을 한다.
	 * {@link TRowLock#prewritten} 에 쓰여 있는 값을 읽어서 prewrite 시에 put 되었던 데이터를 알아낼 수 있다.
	 * currentTimestamp 에 해당하는 값만 지정해서 지워야 한다.
	 *
	 * <p>{@link HTableInterface#checkAndDelete()} 을 통해서 prewritten 들을 지우기 때문에 HBase 에 쓰여진 {@link TRowLock} 은 변하지 않는다.
	 * lock 을 소유하는데 실패하면 {@link ConflictException} 을 throw 한다.
	 * @param rowTxState
	 * @param row
	 * @throws IOException ConflictException, HBase IOException.
	 */
	void deletePrewritten(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException;
}
