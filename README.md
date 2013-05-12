# Haeinsa

Haeinsa is linearly scalable multi-row, multi-table transaction library for HBase.
Haeinsa uses two-phase locking and multi-version concurrency control for implementing transaction.
The isolation level of transaction is serializable.

## Usage

	HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
	HaeinsaTableIface testTable = tablePool.getTable("test");
	HaeinsaTransaction tx = tm.begin();

	HaeinsaPut put1 = new HaeinsaPut(Bytes.toBytes("user1"));
	put.add(Bytes.toBytes("data"), Bytes.toBytes("status"), Bytes.toBytes("Hello World!"));
	testTable.put(tx, put1);
		
	HaeinsaPut put2 = new HaeinsaPut(Bytes.toBytes("user2"));
	put2.add(Bytes.toBytes("data"), Bytes.toBytes("status"), Bytes.toBytes("Linearly Scalable!"));
	testTable.put(tx, put2);

	tx.commit();