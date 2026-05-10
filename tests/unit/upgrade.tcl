start_server {tags {"upgrade external:skip"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    start_server {} {
        set replica [srv 0 client]
        set replica_host [srv 0 host]
        set replica_port [srv 0 port]

        # Set up replication
        $replica slaveof $primary_host $primary_port
        wait_for_condition 50 100 {
            [lindex [$replica role] 0] eq {slave} &&
            [string match {*master_link_status:up*} [$replica info replication]]
        } else {
            fail "Can't turn the instance into a replica"
        }

        test {UPGRADE rejects invalid client ID} {
            catch {$primary upgrade 99999999} err
            assert_match {*No client found*} $err
        }

        test {UPGRADE rejects non-replica client} {
            set my_id [$primary client id]
            catch {$primary upgrade $my_id} err
            assert_match {*not a replica*} $err
        }

        test {UPGRADE basic key migration} {
            # Populate primary with some keys
            for {set i 0} {$i < 100} {incr i} {
                $primary set "key:$i" "value:$i"
            }
            $primary hset myhash f1 v1 f2 v2
            $primary lpush mylist a b c
            $primary sadd myset x y z
            $primary zadd myzset 1.0 a 2.0 b 3.0 c

            # Wait for replication to sync
            wait_for_ofs_sync $replica $primary

            # Find the replica client ID on the primary
            set clients [$primary client list type replica]
            regexp {id=(\d+)} $clients -> replica_client_id

            # Start the upgrade
            set result [$primary upgrade $replica_client_id]
            assert_equal $result {OK}

            # Wait for upgrade to complete
            wait_for_condition 100 100 {
                [dict get [$primary upgrade status] state] eq "done"
            } else {
                set status [$primary upgrade status]
                fail "UPGRADE did not complete. Status: $status"
            }

            # Verify stats
            set status [$primary upgrade status]
            assert {[dict get $status keys_transferred] > 0}
        }

        test {UPGRADE STATUS shows progress} {
            set status [$primary upgrade status]
            assert {[dict exists $status state]}
            assert {[dict exists $status keys_transferred]}
            assert {[dict exists $status bytes_transferred]}
            assert {[dict exists $status elapsed_ms]}
            assert_equal [dict get $status state] "done"
        }

        test {UPGRADE ABORT works} {
            # Add more keys
            for {set i 100} {$i < 200} {incr i} {
                $primary set "key2:$i" "value2:$i"
            }
            wait_for_ofs_sync $replica $primary

            set clients [$primary client list type replica]
            regexp {id=(\d+)} $clients -> replica_client_id

            # Start a new upgrade
            set result [$primary upgrade $replica_client_id]
            assert_equal $result {OK}

            # Immediately abort
            set result [$primary upgrade abort]
            assert_equal $result {OK}

            set status [$primary upgrade status]
            assert_equal [dict get $status state] "aborted"
        }

        test {UPGRADE handles all data types} {
            $primary flushall
            wait_for_ofs_sync $replica $primary

            # String
            $primary set str_key "hello"
            # List
            $primary rpush list_key a b c d e
            # Set
            $primary sadd set_key 1 2 3 4 5
            # Sorted Set
            $primary zadd zset_key 1.0 a 2.0 b 3.0 c
            # Hash
            $primary hset hash_key f1 v1 f2 v2 f3 v3
            # Stream
            $primary xadd stream_key "*" name test value 123
            wait_for_ofs_sync $replica $primary

            set clients [$primary client list type replica]
            regexp {id=(\d+)} $clients -> replica_client_id

            $primary upgrade $replica_client_id

            wait_for_condition 100 100 {
                [dict get [$primary upgrade status] state] eq "done"
            } else {
                fail "UPGRADE did not complete with all data types"
            }

            wait_for_ofs_sync $replica $primary

            assert_equal [$replica get str_key] "hello"
            assert_equal [$replica lrange list_key 0 -1] {a b c d e}
            assert_equal [lsort [$replica smembers set_key]] {1 2 3 4 5}
            assert_equal [$replica zrangebyscore zset_key -inf +inf] {a b c}
            assert_equal [$replica hget hash_key f2] "v2"
            assert {[$replica xlen stream_key] == 1}
        }

        test {UPGRADE handles expiration correctly} {
            $primary flushall
            wait_for_ofs_sync $replica $primary

            # Set keys with TTL
            $primary set "expire:later" "value" EX 3600
            $primary set "no_expire" "value"
            wait_for_ofs_sync $replica $primary

            set clients [$primary client list type replica]
            regexp {id=(\d+)} $clients -> replica_client_id

            $primary upgrade $replica_client_id

            wait_for_condition 100 100 {
                [dict get [$primary upgrade status] state] eq "done"
            } else {
                fail "UPGRADE did not complete"
            }

            wait_for_ofs_sync $replica $primary

            assert_equal [$replica get "expire:later"] "value"
            assert_equal [$replica get "no_expire"] "value"
            assert {[$replica ttl "expire:later"] > 0}
        }

        test {UPGRADE handles large values} {
            $primary flushall
            wait_for_ofs_sync $replica $primary

            # Create a large string value (1MB)
            set bigval [string repeat "x" 1048576]
            $primary set "bigkey" $bigval

            # Create a large hash
            for {set i 0} {$i < 1000} {incr i} {
                $primary hset "bighash" "field:$i" "value:$i"
            }
            wait_for_ofs_sync $replica $primary

            set clients [$primary client list type replica]
            regexp {id=(\d+)} $clients -> replica_client_id

            $primary upgrade $replica_client_id

            wait_for_condition 100 200 {
                [dict get [$primary upgrade status] state] eq "done"
            } else {
                fail "UPGRADE did not complete with large values"
            }

            wait_for_ofs_sync $replica $primary

            assert_equal [string length [$replica get "bigkey"]] 1048576
            assert_equal [$replica hget "bighash" "field:500"] "value:500"
        }

        test {UPGRADE prevents double start} {
            $primary flushall
            for {set i 0} {$i < 1000} {incr i} {
                $primary set "x:$i" $i
            }
            wait_for_ofs_sync $replica $primary

            set clients [$primary client list type replica]
            regexp {id=(\d+)} $clients -> replica_client_id

            $primary upgrade $replica_client_id

            # Try to start another - should fail
            catch {$primary upgrade $replica_client_id} err
            assert_match {*already in progress*} $err

            # Wait for it to finish
            wait_for_condition 100 100 {
                [dict get [$primary upgrade status] state] eq "done"
            } else {
                fail "UPGRADE did not complete"
            }
        }

        test {UPGRADE handles multiple databases} {
            $primary flushall
            wait_for_ofs_sync $replica $primary

            # Write to multiple DBs
            $primary select 0
            $primary set "db0:key" "val0"
            $primary select 1
            $primary set "db1:key" "val1"
            $primary select 2
            $primary set "db2:key" "val2"
            $primary select 0
            wait_for_ofs_sync $replica $primary

            set clients [$primary client list type replica]
            regexp {id=(\d+)} $clients -> replica_client_id

            $primary upgrade $replica_client_id

            wait_for_condition 100 100 {
                [dict get [$primary upgrade status] state] eq "done"
            } else {
                fail "UPGRADE did not complete with multiple DBs"
            }

            wait_for_ofs_sync $replica $primary

            $replica select 0
            assert_equal [$replica get "db0:key"] "val0"
            $replica select 1
            assert_equal [$replica get "db1:key"] "val1"
            $replica select 2
            assert_equal [$replica get "db2:key"] "val2"
            $replica select 0
        }
    }
}
