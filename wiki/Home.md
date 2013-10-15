> Home

Haeinsa is linearly scalable multi-row, multi-table transaction library for [HBase].
Haeinsa uses two-phase locking and optimistic concurrency control for implementing transaction.
The isolation level of transaction is serializable.

### When would I use Haeinsa?

Use Haeinsa if you need strong ACID semantics on your HBase cluster.

### Features

Haeinsa is inspired by Google's [Percolator], but implementation details and isolation level is different.
Haeinsa provides isolation level of serializability, so users can make application based on the assumption
that each transaction executed serially and completely isolated from each other.

- **ACID**: Provides multi-row, multi-table transaction with full ACID senantics.
- **[Linearly scalable](wiki/Performance)**: Can linearly scale out throughput of transaction as scale out your HBase cluster. No theoretical upper limit of throughput.
- **[Serializability]**: Provide isolation level of serializability, which is much better environment to make applications than snapshot isolation.
- **[Low overhead](wiki/Performance)**: Relatively low overhead compared to other comparable libraries.
- **Fault-tolerant**: Haeinsa is fault-tolerant against both client and HBase failures
- **Bare bones HBase**: **NO** additional components, **NO** modification on HBase, client-only library.
- **[Easy migration from HBase application](wiki/Migration-from-HBase)**: Add transaction feature without any change in HBase cluster except adding lock column family.
- **[Used in practice](wiki/Use-Case)**: Haeinsa is used in real service.

### Where name 'Haeinsa' comes from?

The title of this project, [Haeinsa], comes from the name of Buddhism temple in South Korea
which is notable for being home of [Tripitaka Koreana, or Palman Daejanggyeong] which is Buddhist scripture carved in 81,258 wooden printing blocks in 13th century.
It is one of the oldest and comprehensive Buddhist script, and there is no known error or errata in 52,382,960 characters.
We decided to name after because it incarnates important aspects of database which preserve such a massive data without any error for long time.

### Resources

- [Haeinsa Overview Presentation]: Introducing how Haeina works.
- [Announcing Haeinsa]: Blog post of VCNC Engineering Blog (Korean)
- [Haeinsa: Hbase Transaction Library]: Presentation for Deview Conference (Korean)

### License

	Copyright (C) 2013 VCNC Inc.
	
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	
	        http://www.apache.org/licenses/LICENSE-2.0
	
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
	
[HBase]: http://hbase.apache.org/
[Serializability]: http://en.wikipedia.org/wiki/Serializability
[Percolator]: http://research.google.com/pubs/pub36726.html
[Haeinsa]: http://en.wikipedia.org/wiki/Haeinsa
[Tripitaka Koreana, or Palman Daejanggyeong]: http://en.wikipedia.org/wiki/Tripitaka_Koreana
[Haeinsa Overview Presentation]: https://speakerdeck.com/vcnc/haeinsa-overview
[Announcing Haeinsa]: http://engineering.vcnc.co.kr/2013/10/announcing-haeinsa/
[Haeinsa: Hbase Transaction Library]: https://speakerdeck.com/vcnc/haeinsa-hbase-transaction-library
