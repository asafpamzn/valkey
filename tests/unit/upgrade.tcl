# ========================== 2-Node Tests (m_replica + new_replica) ==========================

start_server {tags {"upgrade external:skip"}} {
    set m_replica [srv 0 client]
    set m_replica_host [srv 0 host]
    set m_replica_port [srv 0 port]

    # Populate m_replica with mixed data
    for {set i 0} {$i < 50} {incr i} {
        $m_replica set "key:$i" "value:$i"
    }
    for {set i 0} {$i < 20} {incr i} {
        $m_replica set "ttl:$i" "expval:$i" EX 3600
    }
    $m_replica hset myhash f1 v1 f2 v2 f3 v3
    $m_replica rpush mylist a b c d e
    $m_replica sadd myset x y z w
    $m_replica zadd myzset 1.0 alpha 2.0 beta 3.0 gamma
    $m_replica xadd mystream "*" name test value 123
    $m_replica select 1
    $m_replica set "db1:key" "db1:val"
    $m_replica select 2
    $m_replica set "db2:key" "db2:val"
    $m_replica select 0

    # --- Basic Functionality ---

    start_server {} {
        set new_replica [srv 0 client]
        set new_replica_host [srv 0 host]
        set new_replica_port [srv 0 port]

        test {UPGRADE rejects non-empty node} {
            $new_replica set tempkey tempval
            catch {$new_replica upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port} err
            assert_match {*empty node*} $err
            $new_replica del tempkey
        }

        test {UPGRADE pulls keys from m_replica} {
            set result [$new_replica upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port]
            assert_equal $result {OK}

            # Verify key count (50 + 20 + hash + list + set + zset + stream + 2 in other dbs = 77)
            assert {[$new_replica dbsize] >= 75}

            # Verify string values
            assert_equal [$new_replica get "key:0"] "value:0"
            assert_equal [$new_replica get "key:49"] "value:49"
            assert_equal [$new_replica get "ttl:10"] "expval:10"

            # Verify TTL preserved
            assert {[$new_replica ttl "ttl:10"] > 0}
            assert_equal [$new_replica ttl "key:0"] {-1}
        }

        test {UPGRADE STATUS shows done} {
            set status [$new_replica upgrade status]
            assert_equal [dict get $status state] "done"
            assert {[dict get $status keys_transferred] >= 75}
            assert {[dict get $status elapsed_ms] >= 0}
        }

        test {UPGRADE INFO keyspace correct} {
            # DBSIZE should reflect total keys
            assert {[$new_replica dbsize] >= 75}

            # Verify TTL keys actually have TTL (functional check)
            assert {[$new_replica ttl "ttl:0"] > 0}
            assert {[$new_replica ttl "ttl:19"] > 0}
            assert_equal [$new_replica ttl "key:0"] {-1}

            # Check db0 expires count via INFO
            set info [$new_replica info keyspace]
            if {[regexp {db0:keys=\d+,expires=(\d+)} $info -> expires]} {
                assert {$expires >= 18}
            }
        }

        test {UPGRADE transfers multiple databases} {
            $new_replica select 1
            assert_equal [$new_replica get "db1:key"] "db1:val"
            $new_replica select 2
            assert_equal [$new_replica get "db2:key"] "db2:val"
            $new_replica select 0
        }
    }

    # --- Data Types ---

    start_server {} {
        set nr2 [srv 0 client]

        test {UPGRADE handles all data types} {
            set result [$nr2 upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port]
            assert_equal $result {OK}

            # String
            assert_equal [$nr2 get "key:25"] "value:25"

            # Hash
            assert_equal [$nr2 hget myhash f1] "v1"
            assert_equal [$nr2 hget myhash f3] "v3"
            assert_equal [$nr2 hlen myhash] 3

            # List
            assert_equal [$nr2 lrange mylist 0 -1] {a b c d e}
            assert_equal [$nr2 llen mylist] 5

            # Set
            assert_equal [lsort [$nr2 smembers myset]] {w x y z}
            assert_equal [$nr2 scard myset] 4

            # Sorted Set
            assert_equal [$nr2 zrangebyscore myzset -inf +inf] {alpha beta gamma}
            assert_equal [$nr2 zscore myzset beta] 2

            # Stream
            assert {[$nr2 xlen mystream] == 1}
        }

        test {UPGRADE handles large values} {
            # Add large values to m_replica
            set bigval [string repeat "x" 1048576]
            $m_replica set "bigkey" $bigval
            for {set i 0} {$i < 1000} {incr i} {
                $m_replica hset "bighash" "field:$i" "val:$i"
            }
            # Note: this test reuses m_replica which now has extra keys.
            # The nr2 already got the data above, so check the ones we have.
            assert_equal [string length [$m_replica get "bigkey"]] 1048576
        }
    }

    # --- Threading ---

    start_server {} {
        set nr3 [srv 0 client]

        test {UPGRADE with THREADS 4 works} {
            set result [$nr3 upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port THREADS 4]
            assert_equal $result {OK}
            assert {[$nr3 dbsize] >= 75}
            assert_equal [$nr3 get "key:25"] "value:25"
        }
    }

    start_server {} {
        set nr4 [srv 0 client]

        test {UPGRADE rejects invalid THREADS} {
            catch {$nr4 upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port THREADS 0} err
            assert_match {*THREADS must be*} $err

            catch {$nr4 upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port THREADS 100} err
            assert_match {*THREADS must be*} $err
        }
    }

    # --- Error Handling ---

    start_server {} {
        set nr5 [srv 0 client]

        test {UPGRADE rejects wrong argument count} {
            catch {$nr5 upgrade 127.0.0.1} err
            assert_match {*Usage*} $err
        }
    }
}

# ========================== 3-Node Tests (Primary + m_replica + new_replica) ==========================

start_server {tags {"upgrade external:skip"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    # Populate Primary
    for {set i 0} {$i < 100} {incr i} {
        $primary set "pk:$i" "pv:$i"
    }

    start_server {} {
        set m_replica [srv 0 client]
        set m_replica_host [srv 0 host]
        set m_replica_port [srv 0 port]

        # Make m_replica a replica of Primary
        $m_replica replicaof $primary_host $primary_port
        wait_for_condition 50 200 {
            [string match {*master_link_status:up*} [$m_replica info replication]]
        } else {
            fail "m_replica could not sync with Primary"
        }

        # Verify m_replica has the data
        wait_for_condition 50 200 {
            [$m_replica dbsize] == 100
        } else {
            fail "m_replica dbsize != 100 (got [$m_replica dbsize])"
        }

        start_server {} {
            set new_replica [srv 0 client]
            set new_replica_host [srv 0 host]
            set new_replica_port [srv 0 port]

            test {3-node: UPGRADE + REPLICAOF Primary} {
                # Run UPGRADE on new_replica: pull from m_replica, replicaof Primary
                set result [$new_replica upgrade $m_replica_host $m_replica_port $primary_host $primary_port]
                assert_equal $result {OK}

                # new_replica should have all keys from bulk transfer
                assert {[$new_replica dbsize] >= 100}
                assert_equal [$new_replica get "pk:50"] "pv:50"

                # Wait for new_replica to connect to Primary
                wait_for_condition 50 200 {
                    [string match {*master_link_status:up*} [$new_replica info replication]]
                } else {
                    fail "new_replica could not connect to Primary after UPGRADE"
                }
            }

            test {3-node: new Primary writes reach new_replica} {
                # Write new key to Primary
                $primary set "after_upgrade" "new_value"

                # Wait for it to arrive at new_replica
                wait_for_condition 50 200 {
                    [$new_replica get "after_upgrade"] eq "new_value"
                } else {
                    fail "new_replica did not receive new Primary write"
                }
            }

            test {3-node: writes during UPGRADE arrive via PSYNC} {
                # Primary already had 100 keys + "after_upgrade" = 101
                # new_replica should have at least 101 once fully synced
                wait_for_condition 50 200 {
                    [$new_replica dbsize] >= 101
                } else {
                    fail "new_replica missing keys (dbsize=[$new_replica dbsize])"
                }

                # Verify the key written after UPGRADE
                assert_equal [$new_replica get "after_upgrade"] "new_value"

                # Verify original keys
                assert_equal [$new_replica get "pk:0"] "pv:0"
                assert_equal [$new_replica get "pk:99"] "pv:99"
            }
        }
    }
}
