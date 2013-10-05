> [[Home]] ▸ **Performance**

### Linear Scalability

Throughput of Haeinsa transaction scales out as underlying HBase cluster scales out.
Linear scalability of throughput is tested against HBase cluster on AWS which have XX ECU until XX,XXX ECU.
It is worth to mention transaction shows consistently low latency even when size of cluster become bigger.

TBD - ( ECU vs throughput )( ECU vs latency )

### Latency & Throughput Overhead

Haeinsa transaction has some latency and throughput overhead compared to raw HBase-only operations.
These overhead comes from additional CheckAndPut and Get operations when committing transaction.
Because Haeinsa use row-level locking, overhead varies on schema of database.
For example, one of major transaction used in practice consists of 4 puts and 3 gets across 3 different rows,
experienced 6% latency overhead and 10% throughput overhead after migration.
Even in the worst case, Haeinsa transaction is slower than raw HBase-only operations only by 2~3 times.

TBD - (Haeinsa vs raw HBase, latency and throughput)

### Conflict Rates

TBD - (Conflict rates)
