# ========================== 2-Node Tests (m_replica + new_replica) ==========================

# --- Basic Functionality ---

start_server {tags {"upgrade external:skip needs:debug"}} {
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
}

# --- Data Types ---

start_server {tags {"upgrade external:skip needs:debug"}} {
    set m_replica [srv 0 client]
    set m_replica_host [srv 0 host]
    set m_replica_port [srv 0 port]

    for {set i 0} {$i < 50} {incr i} {
        $m_replica set "key:$i" "value:$i"
    }
    $m_replica hset myhash f1 v1 f2 v2 f3 v3
    $m_replica rpush mylist a b c d e
    $m_replica sadd myset x y z w
    $m_replica zadd myzset 1.0 alpha 2.0 beta 3.0 gamma
    $m_replica xadd mystream "*" name test value 123

    start_server {} {
        set new_replica [srv 0 client]

        test {UPGRADE handles all data types} {
            set result [$new_replica upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port]
            assert_equal $result {OK}

            # String
            assert_equal [$new_replica get "key:25"] "value:25"

            # Hash
            assert_equal [$new_replica hget myhash f1] "v1"
            assert_equal [$new_replica hget myhash f3] "v3"
            assert_equal [$new_replica hlen myhash] 3

            # List
            assert_equal [$new_replica lrange mylist 0 -1] {a b c d e}
            assert_equal [$new_replica llen mylist] 5

            # Set
            assert_equal [lsort [$new_replica smembers myset]] {w x y z}
            assert_equal [$new_replica scard myset] 4

            # Sorted Set
            assert_equal [$new_replica zrangebyscore myzset -inf +inf] {alpha beta gamma}
            assert_equal [$new_replica zscore myzset beta] 2

            # Stream
            assert {[$new_replica xlen mystream] == 1}
        }
    }
}

# --- Threading ---

start_server {tags {"upgrade external:skip needs:debug"}} {
    set m_replica [srv 0 client]
    set m_replica_host [srv 0 host]
    set m_replica_port [srv 0 port]

    for {set i 0} {$i < 50} {incr i} {
        $m_replica set "key:$i" "value:$i"
    }
    for {set i 0} {$i < 20} {incr i} {
        $m_replica set "ttl:$i" "expval:$i" EX 3600
    }
    $m_replica hset myhash f1 v1 f2 v2 f3 v3

    start_server {} {
        set new_replica [srv 0 client]

        test {UPGRADE with THREADS 4 works} {
            set result [$new_replica upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port THREADS 4]
            assert_equal $result {OK}
            assert {[$new_replica dbsize] >= 70}
            assert_equal [$new_replica get "key:25"] "value:25"
        }
    }
}

# --- Error Handling ---

start_server {tags {"upgrade external:skip needs:debug"}} {
    set m_replica [srv 0 client]
    set m_replica_host [srv 0 host]
    set m_replica_port [srv 0 port]

    for {set i 0} {$i < 50} {incr i} {
        $m_replica set "key:$i" "value:$i"
    }

    start_server {} {
        set new_replica [srv 0 client]

        test {UPGRADE rejects wrong argument count} {
            catch {$new_replica upgrade 127.0.0.1} err
            assert_match {*Usage*} $err
        }

        test {UPGRADE rejects unreachable host} {
            # Port 1 is unlikely to have a Valkey server
            catch {$new_replica upgrade 127.0.0.1 1 127.0.0.1 1} err
            assert_match {*ERR*} $err
        }

        test {UPGRADE rejects invalid THREADS} {
            catch {$new_replica upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port THREADS 0} err
            assert_match {*THREADS must be*} $err

            catch {$new_replica upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port THREADS 100} err
            assert_match {*THREADS must be*} $err
        }

        test {UPGRADE rejects second invocation on non-empty node} {
            # First upgrade succeeds
            set result [$new_replica upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port]
            assert_equal $result {OK}

            # Second upgrade should fail (node is no longer empty)
            catch {$new_replica upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port} err
            assert_match {*empty node*} $err
        }
    }
}

# --- Empty source ---

start_server {tags {"upgrade external:skip"}} {
    set empty_source [srv 0 client]
    set empty_source_host [srv 0 host]
    set empty_source_port [srv 0 port]

    start_server {} {
        set new_replica [srv 0 client]

        test {UPGRADE with empty source completes with 0 keys} {
            set result [$new_replica upgrade $empty_source_host $empty_source_port $empty_source_host $empty_source_port]
            assert_equal $result {OK}
            assert_equal [$new_replica dbsize] 0

            set status [$new_replica upgrade status]
            assert_equal [dict get $status state] "done"
            assert_equal [dict get $status keys_transferred] 0
        }
    }
}

# --- 2-Node: Large Values (standalone) ---

start_server {tags {"upgrade external:skip"}} {
    set m_replica [srv 0 client]
    set m_replica_host [srv 0 host]
    set m_replica_port [srv 0 port]

    # Create 1MB string and large hash on m_replica
    $m_replica set "bigkey" [string repeat "x" 1048576]
    for {set i 0} {$i < 1000} {incr i} {
        $m_replica hset "bighash" "field:$i" "val:$i"
    }
    # Also some normal keys
    for {set i 0} {$i < 50} {incr i} {
        $m_replica set "normal:$i" "nval:$i"
    }

    start_server {} {
        set new_replica [srv 0 client]

        test {UPGRADE handles large values} {
            set result [$new_replica upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port]
            assert_equal $result {OK}

            wait_for_condition 50 200 {
                [catch {$new_replica ping} e] == 0
            } else {
                fail "new_replica not responding after upgrade"
            }

            $new_replica replicaof no one

            # Verify the large string transferred correctly
            assert_equal [string length [$new_replica get "bigkey"]] 1048576
            # Verify the large hash transferred correctly
            assert_equal [$new_replica hlen "bighash"] 1000
            assert_equal [$new_replica hget "bighash" "field:500"] "val:500"
            # Verify normal keys too
            assert_equal [$new_replica get "normal:25"] "nval:25"
        }
    }
}

# ========================== 3-Node Tests (Primary + m_replica + new_replica) ==========================

start_server {tags {"upgrade external:skip needs:repl"}} {
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

            test {3-node: UPGRADE uses partial sync (not full sync)} {
                # The new_replica should have received the correct replid via REPLINFO
                # and connected to Primary via partial sync, not a full resync.
                # Check sync_full on primary: should be 1 (m_replica only), not 2.
                set stats [$primary info stats]
                regexp {sync_full:([^\r\n]+)} $stats -> sync_full
                assert_equal $sync_full 1
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
