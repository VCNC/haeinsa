# Haeinsa

[![Build Status](https://travis-ci.org/VCNC/haeinsa.svg?branch=master)](https://travis-ci.org/VCNC/haeinsa)

Haeinsa is linearly scalable multi-row, multi-table transaction library for HBase.
Haeinsa uses two-phase locking and optimistic concurrency control for implementing transaction.
The isolation level of transaction is serializable.

## Features

Please see Haeinsa [Wiki] for further information.

- **ACID**: Provides multi-row, multi-table transaction with full ACID semantics.
- **[Linearly scalable]**: Can linearly scale out throughput of transaction as scale out your HBase cluster.
- **[Serializability]**: Provide isolation level of serializability.
- **[Low overhead]**: Relatively low overhead compared to other comparable libraries.
- **Fault-tolerant**: Haeinsa is fault-tolerant against both client and HBase failures.
- **[Easy migration]**: Add transaction feature to your own HBase cluster without any change in HBase cluster except adding lock column family.
- **[Used in practice]**: Haeinsa is used in real service.

## Usage

APIs of Haeinsa is really similar to APIs of HBase. Please see [How to Use] and [API Usage] document for further information.

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

## Resources

- [Haeinsa Overview Presentation]: Introducing how Haeina works.
- [Announcing Haeinsa]: Blog post of VCNC Engineering Blog (Korean)
- [Haeinsa: Hbase Transaction Library]: Presentation for Deview Conference (Korean)

## License

	Copyright (C) 2013-2015 VCNC Inc.
	
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	
	        http://www.apache.org/licenses/LICENSE-2.0
	
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.

[Wiki]: https://github.com/vcnc/haeinsa/wiki
[How to Use]: https://github.com/vcnc/haeinsa/wiki/How-to-Use
[API Usage]: https://github.com/vcnc/haeinsa/wiki/API-Usage
[HBase]: http://hbase.apache.org/
[Serializability]: http://en.wikipedia.org/wiki/Serializability
[Percolator]: http://research.google.com/pubs/pub36726.html
[Haeinsa]: http://en.wikipedia.org/wiki/Haeinsa
[Tripitaka Koreana, or Palman Daejanggyeong]: http://en.wikipedia.org/wiki/Tripitaka_Koreana
[Haeinsa Overview Presentation]: https://speakerdeck.com/vcnc/haeinsa-overview-hbase-transaction-library
[Announcing Haeinsa]: http://engineering.vcnc.co.kr/2013/10/announcing-haeinsa/
[Linearly scalable]: https://github.com/vcnc/haeinsa/wiki/Performance
[Low overhead]: https://github.com/vcnc/haeinsa/wiki/Performance
[Easy Migration]: https://github.com/vcnc/haeinsa/wiki/Migration-from-HBase
[Used in practice]: https://github.com/vcnc/haeinsa/wiki/Use-Case
[Haeinsa: Hbase Transaction Library]: https://speakerdeck.com/vcnc/haeinsa-hbase-transaction-library
