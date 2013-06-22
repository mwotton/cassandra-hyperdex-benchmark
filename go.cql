CREATE KEYSPACE testkeyspace WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'datacenter1': '1'
};

USE testkeyspace;

CREATE TABLE phonebook (
  key text PRIMARY KEY,
  first text,
  last text
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'SnappyCompressor'};
