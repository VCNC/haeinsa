> [[Home]] ▸ **API Usage**

API of Haeinsa is really similar to HBase APIs.
Here is some sample codes using Haeinsa library for serveral operations.

### Get

Haeinsa provides transaction on Get operation.
This transaction put some data on specific row and then get data from same row:

	HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
	HaeinsaTableIface table = tablePool.getTable("test");
	byte[] row = Bytes.toBytes("user");
	byte[] family = Bytes.toBytes("data");
	byte[] qualifier = Bytes.toBytes("status");
	
	HaeinsaTransaction tx = tm.begin(); // start transaction
	
	HaeinsaGet put = new HaeinsaPut(row);
	put.add(family, qualifier, Bytes.toBytes("Hello World!"));
	table.put(tx, put);
	
	HaeinsaGet get = new HaeinsaGet(row);
	get.addColumn(family, qualifier);
	HaeinsaResult result = table.get(tx, get);
	byte[] value = result.getValue(row, family, qualifier);
	
	tx.commit(); // commit transaction to HBase

### Put

Haeinsa provides transaction on Put operation.
This transaction put to multiple rows atomically:

	HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
	HaeinsaTableIface table = tablePool.getTable("test");
	byte[] family = Bytes.toBytes("data");
	byte[] qualifier = Bytes.toBytes("status");
	
	HaeinsaTransaction tx = tm.begin(); // start transaction
	
	HaeinsaPut put1 = new HaeinsaPut(Bytes.toBytes("user1"));
	put1.add(family, qualifier, Bytes.toBytes("Hello World!"));
	table.put(tx, put1);
	
	HaeinsaPut put2 = new HaeinsaPut(Bytes.toBytes("user2"));
	put2.add(family, qualifier, Bytes.toBytes("Linearly Scalable!"));
	table.put(tx, put2);
	
	tx.commit(); // commit transaction to HBase

### Delete

Haeinsa provides transaction on Delete operation.
This transaction get data from specific row and then, delete the data:

	HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
	HaeinsaTableIface table = tablePool.getTable("test");
	byte[] row = Bytes.toBytes("user");
	byte[] family = Bytes.toBytes("data");
	byte[] qualifier = Bytes.toBytes("status");
	
	HaeinsaTransaction tx = tm.begin(); // start transaction
	
	HaeinsaGet get = new HaeinsaGet(row);
	get.addColumn(family, qualifier);
	HaeinsaResult result = table.get(tx, get);
	byte[] value = result.getValue(row, family, qualifier);
	
	HaeinsaDelete delete = new HaeinsaDelete(row);
	delete.deleteColumns(row, family);
	table.delete(tx, delete);
	
	table.delete(tx, delete);

	tx.commit(); // commit transaction to HBase

### Intra-row scan

Haeinsa provides transaction on intra-row scan operation.
Intra-row scan is operation which scans columns of single row.
Intra-row scan operation is much better in performance than scanning multiple rows.
This transaction performs intra-row scan and put atomically:

	HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
	HaeinsaTableIface table = tablePool.getTable("test");
	
	HaeinsaTransaction tx = tm.begin(); // start transaction
	
	HaeinsaIntraScan intraScan = new HaeinsaIntraScan(
	        Bytes.toBytes("user1"),
	        Bytes.toBytes("column0"), true,
	        Bytes.toBytes("column9"), true);

	HaeinsaResultScanner resultScanner = table.getScanner(tx, intraScan);
	Iterator<HaeinsaResult> iter = resultScanner.iterator();
	while(iter.hasNext()) {
		HaeinsaResult result = iter.next();
		// do something with result
	}
	resultScanner.close();
	
	HaeinsaPut put = new HaeinsaPut(Bytes.toBytes("user1"));
	put.add(Bytes.toBytes("data"), Bytes.toBytes("status"), Bytes.toBytes("Scan completed!"));
	testTable.put(tx, put);
	
	tx.commit(); // commit transaction to HBase

### Inter-row scan

Haeinsa provides transaction on inter-row scan operation.
Inter-row scan is operation which scans multiple rows.
This transaction performs inter row scan and put atomically:

	HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
	HaeinsaTableIface table = tablePool.getTable("test");
	
	HaeinsaTransaction tx = tm.begin(); // start transaction
	
	HaeinsaScan scan = new HaeinsaScan();
	scan.setStartRow(Bytes.toBytes("user1"));
	scan.setStopRow(Bytes.toBytes("user9"));
	HaeinsaResultScanner resultScanner = testTable.getScanner(tx, scan)
	Iterator<HaeinsaResult> iter = resultScanner.iterator();
	while (iter.hasNext()) {
	    HaeinsaResult result = iter.next();
	// do something with result
	}
	resultScanner.close();
	
	HaeinsaPut put = new HaeinsaPut(Bytes.toBytes("user1"));
	put.add(Bytes.toBytes("data"), Bytes.toBytes("status"), Bytes.toBytes("Scan completed!"));
	testTable.put(tx, put);
	
	tx.commit(); // commit transaction to HBase
