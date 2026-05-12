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

# ========================== Long-Running / Large Data Tests ==========================
# These tests exercise the UPGRADE command with >= 1GB of data to validate
# correctness and stability under realistic production-like data volumes.

# --- 2-Node: 1GB bulk transfer (100K keys × 10KB values) ---

start_server {tags {"upgrade external:skip slow"}} {
    set m_replica [srv 0 client]
    set m_replica_host [srv 0 host]
    set m_replica_port [srv 0 port]

    # Populate m_replica with ~1GB of data: 100,000 keys with 10KB values
    $m_replica debug populate 100000 bigdata 10000

    # Also add keys with TTLs (10K keys with TTL)
    for {set i 0} {$i < 10000} {incr i} {
        $m_replica set "ttlbig:$i" [string repeat "t" 1024] EX 7200
    }

    start_server {} {
        set new_replica [srv 0 client]
        set new_replica_host [srv 0 host]
        set new_replica_port [srv 0 port]

        test {UPGRADE 1GB: bulk transfer completes successfully} {
            set start [clock milliseconds]
            set result [$new_replica upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port THREADS 8]
            set elapsed [expr {[clock milliseconds] - $start}]
            assert_equal $result {OK}

            # Should have all 110K keys
            assert {[$new_replica dbsize] >= 110000}

            # Verify random samples (debug populate format is "prefix:N")
            assert_equal [string length [$new_replica get "bigdata:50000"]] 10000
            assert_equal [string length [$new_replica get "bigdata:99999"]] 10000

            # Verify TTL keys
            assert {[$new_replica ttl "ttlbig:5000"] > 0}
            assert {[$new_replica ttl "ttlbig:9999"] > 0}

            # Log timing for visibility
            puts "UPGRADE 1GB (2-node): $elapsed ms for [$new_replica dbsize] keys"
        }

        test {UPGRADE 1GB: STATUS reports correct totals} {
            set status [$new_replica upgrade status]
            assert_equal [dict get $status state] "done"
            assert {[dict get $status keys_transferred] >= 110000}
            # 100K keys × ~10KB payload + 10K × ~1KB = ~1GB+
            assert {[dict get $status bytes_transferred] > 1000000000}
            assert {[dict get $status elapsed_ms] > 0}
        }

        test {UPGRADE 1GB: data integrity spot checks} {
            # Check value lengths are correct for debug populate keys (10KB each)
            for {set i 0} {$i < 100} {incr i} {
                set idx [expr {int(rand() * 100000)}]
                set val [$new_replica get "bigdata:$idx"]
                assert_equal [string length $val] 10000
            }

            # Check TTL keys have correct values and TTL
            for {set i 0} {$i < 50} {incr i} {
                set idx [expr {int(rand() * 10000)}]
                assert_equal [string length [$new_replica get "ttlbig:$idx"]] 1024
                assert {[$new_replica ttl "ttlbig:$idx"] > 0}
            }
        }

        test {UPGRADE 1GB: INFO keyspace expires count correct} {
            set info [$new_replica info keyspace]
            if {[regexp {db0:keys=(\d+),expires=(\d+)} $info -> keys expires]} {
                assert {$keys >= 110000}
                assert {$expires >= 10000}
            } else {
                fail "Could not parse INFO keyspace"
            }
        }
    }
}

# --- 2-Node: 1GB with multi-threaded (THREADS 1 vs 4 vs 8) ---

start_server {tags {"upgrade external:skip slow"}} {
    set m_replica [srv 0 client]
    set m_replica_host [srv 0 host]
    set m_replica_port [srv 0 port]

    # ~1GB: 50K keys × 20KB values
    $m_replica debug populate 50000 mt 20000

    start_server {} {
        set nr [srv 0 client]

        test {UPGRADE 1GB: single-threaded baseline timing} {
            set start [clock milliseconds]
            set result [$nr upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port THREADS 1]
            set elapsed_1t [expr {[clock milliseconds] - $start}]
            assert_equal $result {OK}
            assert {[$nr dbsize] >= 50000}
            puts "UPGRADE 1GB (1 thread): $elapsed_1t ms"
        }
    }

    start_server {} {
        set nr [srv 0 client]

        test {UPGRADE 1GB: 4 threads faster than 1 thread} {
            set start [clock milliseconds]
            set result [$nr upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port THREADS 4]
            set elapsed_4t [expr {[clock milliseconds] - $start}]
            assert_equal $result {OK}
            assert {[$nr dbsize] >= 50000}
            puts "UPGRADE 1GB (4 threads): $elapsed_4t ms"
        }
    }

    start_server {} {
        set nr [srv 0 client]

        test {UPGRADE 1GB: 8 threads completes successfully} {
            set start [clock milliseconds]
            set result [$nr upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port THREADS 8]
            set elapsed_8t [expr {[clock milliseconds] - $start}]
            assert_equal $result {OK}
            assert {[$nr dbsize] >= 50000}

            # Verify data integrity with 8 threads
            for {set i 0} {$i < 100} {incr i} {
                set idx [expr {int(rand() * 50000)}]
                assert_equal [string length [$nr get "mt:$idx"]] 20000
            }
            puts "UPGRADE 1GB (8 threads): $elapsed_8t ms"
        }
    }
}

# --- 2-Node: Mixed data types at scale (>1GB total) ---

start_server {tags {"upgrade external:skip slow"}} {
    set m_replica [srv 0 client]
    set m_replica_host [srv 0 host]
    set m_replica_port [srv 0 port]

    # Strings: 50K keys × 10KB = ~500MB
    $m_replica debug populate 50000 str 10000

    # Hashes: 5K hashes with 100 fields × 1KB values = ~500MB
    for {set i 0} {$i < 5000} {incr i} {
        set args [list]
        for {set f 0} {$f < 100} {incr f} {
            lappend args "field:$f" [string repeat "h" 1000]
        }
        $m_replica hset "hash:$i" {*}$args
    }

    # Lists: 1K lists with 1000 elements × 100 bytes = ~100MB
    for {set i 0} {$i < 1000} {incr i} {
        set args [list]
        for {set e 0} {$e < 1000} {incr e} {
            lappend args [string repeat "l" 100]
        }
        $m_replica rpush "list:$i" {*}$args
    }

    # Sorted sets: 1K zsets with 500 members = ~50MB
    for {set i 0} {$i < 1000} {incr i} {
        set args [list]
        for {set m 0} {$m < 500} {incr m} {
            lappend args [expr {$m * 1.5}] "member:$m:[string repeat z 50]"
        }
        $m_replica zadd "zset:$i" {*}$args
    }

    # Sets: 1K sets with 500 members = ~25MB
    for {set i 0} {$i < 1000} {incr i} {
        set args [list]
        for {set m 0} {$m < 500} {incr m} {
            lappend args "elem:$m:[string repeat s 50]"
        }
        $m_replica sadd "set:$i" {*}$args
    }

    start_server {} {
        set nr [srv 0 client]

        test {UPGRADE 1GB mixed types: all data types transferred correctly} {
            set result [$nr upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port THREADS 8]
            assert_equal $result {OK}

            # Total keys: 50K strings + 5K hashes + 1K lists + 1K zsets + 1K sets = 58K
            assert {[$nr dbsize] >= 58000}

            # Verify strings (debug populate format: "str:N")
            for {set i 0} {$i < 20} {incr i} {
                set idx [expr {int(rand() * 50000)}]
                assert_equal [string length [$nr get "str:$idx"]] 10000
            }

            # Verify hashes
            for {set i 0} {$i < 20} {incr i} {
                set idx [expr {int(rand() * 5000)}]
                assert_equal [$nr hlen "hash:$idx"] 100
                assert_equal [string length [$nr hget "hash:$idx" "field:50"]] 1000
            }

            # Verify lists
            for {set i 0} {$i < 20} {incr i} {
                set idx [expr {int(rand() * 1000)}]
                assert_equal [$nr llen "list:$idx"] 1000
                assert_equal [string length [$nr lindex "list:$idx" 500]] 100
            }

            # Verify sorted sets
            for {set i 0} {$i < 20} {incr i} {
                set idx [expr {int(rand() * 1000)}]
                assert_equal [$nr zcard "zset:$idx"] 500
            }

            # Verify sets
            for {set i 0} {$i < 20} {incr i} {
                set idx [expr {int(rand() * 1000)}]
                assert_equal [$nr scard "set:$idx"] 500
            }
        }
    }
}

# --- 3-Node: 1GB with PSYNC verification ---

start_server {tags {"upgrade external:skip slow"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    # Populate primary with ~1GB: 100K keys × 10KB values
    $primary debug populate 100000 pk 10000

    start_server {} {
        set m_replica [srv 0 client]
        set m_replica_host [srv 0 host]
        set m_replica_port [srv 0 port]

        # Make m_replica a replica of primary
        $m_replica replicaof $primary_host $primary_port
        wait_for_condition 100 500 {
            [string match {*master_link_status:up*} [$m_replica info replication]]
        } else {
            fail "m_replica could not sync with Primary"
        }

        # Wait for full sync to complete (1GB takes time)
        wait_for_condition 300 1000 {
            [$m_replica dbsize] == 100000
        } else {
            fail "m_replica dbsize != 100000 (got [$m_replica dbsize])"
        }

        start_server {} {
            set new_replica [srv 0 client]
            set new_replica_host [srv 0 host]
            set new_replica_port [srv 0 port]

            test {3-node 1GB: UPGRADE completes and PSYNC succeeds} {
                set start [clock milliseconds]
                set result [$new_replica upgrade $m_replica_host $m_replica_port $primary_host $primary_port THREADS 8]
                set elapsed [expr {[clock milliseconds] - $start}]
                assert_equal $result {OK}

                assert {[$new_replica dbsize] >= 100000}
                puts "UPGRADE 1GB (3-node): $elapsed ms"

                # Wait for new_replica to connect to Primary via PSYNC
                wait_for_condition 100 500 {
                    [string match {*master_link_status:up*} [$new_replica info replication]]
                } else {
                    fail "new_replica could not connect to Primary after UPGRADE"
                }
            }

            test {3-node 1GB: partial sync used (not full sync)} {
                set stats [$primary info stats]
                regexp {sync_full:([^\r\n]+)} $stats -> sync_full
                # sync_full should be 1 (only m_replica did full sync)
                assert_equal $sync_full 1
            }

            test {3-node 1GB: replid matches primary} {
                set primary_info [$primary info replication]
                regexp {master_replid:([^\r\n]+)} $primary_info -> primary_replid

                set nr_info [$new_replica info replication]
                regexp {master_replid:([^\r\n]+)} $nr_info -> nr_replid

                assert_equal $primary_replid $nr_replid
            }

            test {3-node 1GB: new writes replicate after upgrade} {
                # Write 1000 new keys to primary after upgrade
                for {set i 0} {$i < 1000} {incr i} {
                    $primary set "post_upgrade:$i" [string repeat "n" 1000]
                }

                # Wait for replication to catch up
                wait_for_condition 100 500 {
                    [$new_replica get "post_upgrade:999"] eq [string repeat "n" 1000]
                } else {
                    fail "new_replica did not receive post-upgrade writes"
                }

                # Verify all post-upgrade keys
                for {set i 0} {$i < 100} {incr i} {
                    set idx [expr {int(rand() * 1000)}]
                    assert_equal [$new_replica get "post_upgrade:$idx"] [string repeat "n" 1000]
                }
            }

            test {3-node 1GB: data integrity after full cycle} {
                # Verify original keys still correct (debug populate format: "pk:N")
                for {set i 0} {$i < 100} {incr i} {
                    set idx [expr {int(rand() * 100000)}]
                    assert_equal [string length [$new_replica get "pk:$idx"]] 10000
                }
            }
        }
    }
}

# --- 3-Node: Writes during UPGRADE (primary writes while bulk transfer runs) ---

start_server {tags {"upgrade external:skip slow"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    # Populate primary with ~500MB: 50K keys × 10KB
    $primary debug populate 50000 base 10000

    start_server {} {
        set m_replica [srv 0 client]
        set m_replica_host [srv 0 host]
        set m_replica_port [srv 0 port]

        $m_replica replicaof $primary_host $primary_port
        wait_for_condition 100 500 {
            [string match {*master_link_status:up*} [$m_replica info replication]]
        } else {
            fail "m_replica could not sync with Primary"
        }

        wait_for_condition 300 1000 {
            [$m_replica dbsize] == 50000
        } else {
            fail "m_replica dbsize != 50000 (got [$m_replica dbsize])"
        }

        # Now write additional data to primary WHILE upgrade will be running.
        # The delta phase should capture these writes.

        start_server {} {
            set new_replica [srv 0 client]
            set new_replica_host [srv 0 host]
            set new_replica_port [srv 0 port]

            test {3-node concurrent writes: writes during upgrade arrive via delta+PSYNC} {
                # Start background write load on primary (writes for 30 seconds)
                set load_handle [start_write_load $primary_host $primary_port 30]

                # Run upgrade (this takes time with 50K×10KB = 500MB)
                set result [$new_replica upgrade $m_replica_host $m_replica_port $primary_host $primary_port THREADS 4]
                assert_equal $result {OK}

                # Stop the writer
                stop_write_load $load_handle

                # Wait for connection to primary
                wait_for_condition 100 500 {
                    [string match {*master_link_status:up*} [$new_replica info replication]]
                } else {
                    fail "new_replica not connected to primary"
                }

                # Give replication time to deliver remaining writes
                after 2000

                # new_replica should have at least the original 50K keys
                assert {[$new_replica dbsize] >= 50000}

                # Write some new keys AFTER upgrade to verify replication works
                for {set i 0} {$i < 100} {incr i} {
                    $primary set "post_concurrent:$i" "val:$i"
                }

                # Verify post-upgrade writes arrive
                wait_for_condition 100 500 {
                    [$new_replica get "post_concurrent:99"] eq "val:99"
                } else {
                    fail "post-upgrade writes not replicated"
                }
            }
        }
    }
}

# --- 2-Node: m_replica responsiveness during large transfer ---

start_server {tags {"upgrade external:skip slow"}} {
    set m_replica [srv 0 client]
    set m_replica_host [srv 0 host]
    set m_replica_port [srv 0 port]

    # ~1GB data
    $m_replica debug populate 100000 resp 10000

    start_server {} {
        set new_replica [srv 0 client]
        set new_replica_host [srv 0 host]
        set new_replica_port [srv 0 port]

        test {UPGRADE 1GB: m_replica remains responsive during transfer} {
            # Start upgrade via a deferred client so we don't block
            set bg_client [valkey_deferring_client_by_addr $new_replica_host $new_replica_port]
            $bg_client UPGRADE $m_replica_host $m_replica_port $m_replica_host $m_replica_port THREADS 8

            # Give it a moment to start the transfer
            after 200

            # Repeatedly PING m_replica while transfer is running
            set pings_ok 0
            set pings_total 0
            for {set i 0} {$i < 50} {incr i} {
                incr pings_total
                if {[catch {$m_replica ping} reply] == 0 && $reply eq "PONG"} {
                    incr pings_ok
                }
                after 100
            }

            # m_replica should respond to at least 90% of pings
            assert {$pings_ok >= 45}
            puts "m_replica responsiveness: $pings_ok/$pings_total pings OK during 1GB transfer"

            # Also verify m_replica can serve reads during transfer
            set reads_ok 0
            for {set i 0} {$i < 20} {incr i} {
                set idx [expr {int(rand() * 100000)}]
                if {[catch {$m_replica get "resp:$idx"} val] == 0 && [string length $val] == 10000} {
                    incr reads_ok
                }
                after 50
            }
            assert {$reads_ok >= 18}
            puts "m_replica read responsiveness: $reads_ok/20 reads OK during transfer"

            # Wait for the upgrade to complete (read deferred response)
            set reply [$bg_client read]
            assert_equal $reply {OK}
            $bg_client close

            # Verify transfer completed correctly
            assert {[$new_replica dbsize] >= 100000}
        }
    }
}

# --- 2-Node: Very large values (1MB+ per key) ---

start_server {tags {"upgrade external:skip slow"}} {
    set m_replica [srv 0 client]
    set m_replica_host [srv 0 host]
    set m_replica_port [srv 0 port]

    # 1024 keys × 1MB = 1GB
    for {set i 0} {$i < 1024} {incr i} {
        $m_replica set "huge:$i" [string repeat "M" 1048576]
    }
    # Also add some normal-sized keys
    $m_replica debug populate 10000 normal 100

    start_server {} {
        set nr [srv 0 client]

        test {UPGRADE 1GB: 1MB values transfer correctly} {
            set start [clock milliseconds]
            set result [$nr upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port THREADS 4]
            set elapsed [expr {[clock milliseconds] - $start}]
            assert_equal $result {OK}

            assert {[$nr dbsize] >= 11024}

            # Verify all 1MB values
            for {set i 0} {$i < 1024} {incr i} {
                assert_equal [string length [$nr get "huge:$i"]] 1048576
            }

            # Verify normal keys too (debug populate format: "normal:N")
            for {set i 0} {$i < 50} {incr i} {
                set idx [expr {int(rand() * 10000)}]
                assert_equal [string length [$nr get "normal:$idx"]] 100
            }

            puts "UPGRADE 1GB (1MB values): $elapsed ms for [$nr dbsize] keys"
        }
    }
}

# --- 3-Node: Large data with multiple databases ---

start_server {tags {"upgrade external:skip slow"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    # Spread ~1GB across multiple databases
    # db0: 500MB (50K keys × 10KB)
    $primary debug populate 50000 db0key 10000
    # db1: 300MB (30K keys × 10KB)
    $primary select 1
    $primary debug populate 30000 db1key 10000
    # db2: 200MB (20K keys × 10KB)
    $primary select 2
    $primary debug populate 20000 db2key 10000
    $primary select 0

    start_server {} {
        set m_replica [srv 0 client]
        set m_replica_host [srv 0 host]
        set m_replica_port [srv 0 port]

        $m_replica replicaof $primary_host $primary_port
        wait_for_condition 100 500 {
            [string match {*master_link_status:up*} [$m_replica info replication]]
        } else {
            fail "m_replica could not sync with Primary"
        }

        # Wait for full sync including all DBs
        wait_for_condition 300 1000 {
            [$m_replica dbsize] == 50000
        } else {
            fail "m_replica db0 size != 50000 (got [$m_replica dbsize])"
        }

        # Verify other DBs synced (check db1 has expected keys)
        $m_replica select 1
        wait_for_condition 300 1000 {
            [$m_replica dbsize] == 30000
        } else {
            fail "m_replica db1 not synced (got [$m_replica dbsize])"
        }
        $m_replica select 0

        start_server {} {
            set new_replica [srv 0 client]
            set new_replica_host [srv 0 host]
            set new_replica_port [srv 0 port]

            test {3-node 1GB multi-db: UPGRADE transfers all databases} {
                set result [$new_replica upgrade $m_replica_host $m_replica_port $primary_host $primary_port THREADS 8]
                assert_equal $result {OK}

                # Verify db0 (debug populate format: "db0key:N")
                assert {[$new_replica dbsize] >= 50000}
                set idx [expr {int(rand() * 50000)}]
                assert_equal [string length [$new_replica get "db0key:$idx"]] 10000

                # Verify db1 (debug populate format: "db1key:N")
                $new_replica select 1
                assert {[$new_replica dbsize] >= 30000}
                assert_equal [string length [$new_replica get "db1key:15000"]] 10000

                # Verify db2 (debug populate format: "db2key:N")
                $new_replica select 2
                assert {[$new_replica dbsize] >= 20000}
                assert_equal [string length [$new_replica get "db2key:10000"]] 10000

                $new_replica select 0
            }

            test {3-node 1GB multi-db: PSYNC connects to primary} {
                wait_for_condition 100 500 {
                    [string match {*master_link_status:up*} [$new_replica info replication]]
                } else {
                    fail "new_replica could not connect to Primary"
                }

                # Verify partial sync
                set stats [$primary info stats]
                regexp {sync_full:([^\r\n]+)} $stats -> sync_full
                assert_equal $sync_full 1
            }

            test {3-node 1GB multi-db: post-upgrade writes to all dbs replicate} {
                $primary select 1
                $primary set "post:db1" "new_val_1"
                $primary select 2
                $primary set "post:db2" "new_val_2"
                $primary select 0
                $primary set "post:db0" "new_val_0"

                wait_for_condition 100 500 {
                    [$new_replica get "post:db0"] eq "new_val_0"
                } else {
                    fail "post-upgrade write to db0 not replicated"
                }

                $new_replica select 1
                wait_for_condition 50 200 {
                    [$new_replica get "post:db1"] eq "new_val_1"
                } else {
                    fail "post-upgrade write to db1 not replicated"
                }

                $new_replica select 2
                wait_for_condition 50 200 {
                    [$new_replica get "post:db2"] eq "new_val_2"
                } else {
                    fail "post-upgrade write to db2 not replicated"
                }
                $new_replica select 0
            }
        }
    }
}
