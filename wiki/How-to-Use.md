> [[Home]] ▸ **How it Use**

### Quick Setup

#### 1. Create Lock Column Family

Add "lock" column family to all of tables on your HBase cluster.

#### 2. Include library to your project

You can manually include Haeinsa into your project by adding jar files to classpath.
Or you can simply add maven dependency on your pom.xml if your project is maven project.
Haeinsa JARs are available from Maven central repository.

	<dependency>
	    <groupId>kr.co.vcnc.haeinsa</groupId>
	    <artifactId>haeinsa</artifactId>
	    <version>1.0.0</version>
	</dependency>

#### 3. Use Haeinsa library

Here is sample codes. You can see more sample codes on the [[API Usage]] page.

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

### Important Information

Here is what you need to know before using Haeinsa:

1. Haeinsa is designed for low-conflict environment. If many transactions concurrently access to same row, performance of Haeinsa can be degraded.
2. Haeinsa is targeted for transactions across handful of rows ( from 1 to 100s of rows ), not for transaction against thousands of rows.
3. If transaction take more than 5 second, it might be aborted by other ongoing transaction frequently which can cause poor performance. 
4. It is **highly recommended** to synchronize clock of each client which use Haeinsa within reasonable time skew. (eg. ntpd)
5. User application can't explicitly control timestamp in Haeinsa. Timestamp is managed by Haeinsa transaction manager to implement safe rollback.
6. Dedicated lock column family ("!lock!" by default) has to be the first column family on every table when sorting names of all column family lexicographically. 
	- Most of the case, this requirement can be met easily because '!' is the first printable character in ascii code.
7. HBase version higher than 0.94.3 required
	- This is because consistency bug on previous version of HBase. 
	- Strongly recommended to use version which apply issue [HBASE-7051](https://issues.apache.org/jira/browse/HBASE-7051)
