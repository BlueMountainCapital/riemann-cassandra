# riemann-cassandra : tool for emitting Cassandra metrics to Riemann

## Background

[Riemann] along with [Graphite] make it possible to easily monitor any service.

This tool lets you monitor [Cassandra] by emitting metrics in JMX as Riemann events
You should run one of these per Cassandra node. 

## Usage
``` bash
    java -jar riemann-cassandra-0.0.1.jar  
         -riemann_host <arg>     #defaults to localhost
         -riemann_port <arg>     #defaults to 5555
         -cassandra_host <arg>   #defaults to localhost
         -jmx_port <arg>         #defaults to 7199
         -jmx_username <arg>     #defaults to null
         -jmx_password <arg>     #defaults to null
         -interval_seconds <arg> #defaults to 5
```

## Metrics tracked
  
  * General
    * cassandra.heap_committed_mb
    * cassandra.heap_used_mb
    * cassandra.exception_count
    * cassandra.recent_timeouts
    * cassandra.pending_compactions
    * cassandra.total_sstable_mb


  * Per ThreadPool 
    * cassandra.tp.active
    * cassandra.tp.blocked
    * cassandra.tp.pending


  * Per Keyspace/ColumnFamily
    * cassandra.db.keys
    * cassandra.db.mean_row_size
    * cassandra.db.min_row_size
    * cassandra.db.max_row_size
    * cassandra.db.sstable_count
    * cassandra.db.total_sstable_mb
    * cassandra.db.total_bloom_mb
    * cassamdra.db.bloom_fp_rate
    * cassandra.db.memtable_size_mb
    * cassandra.db.read_latency
    * cassandra.db.write_latency

[Cassandra]:http://cassandra.apache.org
[Riemann]:http://aphyr.github.com/riemann
[Graphite]:http://graphite.wikidot.com
