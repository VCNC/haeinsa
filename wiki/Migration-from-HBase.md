> [[Home]] ▸ **Migration from HBase**

HBase application can be easily migrated to Haeinsa,
because Haeinsa provides similar interface with HBase like `Get`, `Put`, `Scan`, `IntraScan`(column-range scan), and `Delete`.
Haeinsa does not intrude data or schema of original HBase to migrate,
but you have to create special lock column family ("!lock!" by default) for every table.
