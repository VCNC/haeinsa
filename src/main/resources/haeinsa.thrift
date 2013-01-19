namespace java kr.co.vcnc.haeinsa.thrift

enum RowState {
	STABLE = 1,
	PREWRITTEN = 2,
	ABORTED = 3,
	COMMITTED = 4,
}

struct RowKey {
	1: required binary tableName,
	2: required binary row,
}

struct CellKey {
	1: required binary family,
	2: required binary qualifier,
}

struct RowLock {
	1: required i32 version,
	2: required RowState state,
	3: required i64 commitTimestamp,
	4: optional i64 timeout,
	5: optional RowKey primary,
	6: optional list<RowKey> secondaries,
	7: optional list<CellKey> puts,
	8: optional list<CellKey> deletes,
}