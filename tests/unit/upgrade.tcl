start_server {tags {"upgrade external:skip"}} {
    set m_replica [srv 0 client]
    set m_replica_host [srv 0 host]
    set m_replica_port [srv 0 port]

    # Populate m_replica with data
    for {set i 0} {$i < 50} {incr i} {
        $m_replica set "key:$i" "value:$i"
    }
    for {set i 0} {$i < 20} {incr i} {
        $m_replica set "ttl:$i" "expval:$i" EX 3600
    }
    $m_replica hset myhash f1 v1 f2 v2

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
            # Increase client timeout to handle blocking sender
            $new_replica config set timeout 30

            set result [$new_replica upgrade $m_replica_host $m_replica_port $m_replica_host $m_replica_port]
            assert_equal $result {OK}

            # All keys should be present
            assert {[$new_replica dbsize] >= 71}
            assert_equal [$new_replica get "key:0"] "value:0"
            assert_equal [$new_replica get "key:49"] "value:49"
            assert_equal [$new_replica hget myhash f1] "v1"
            assert_equal [$new_replica get "ttl:10"] "expval:10"
            assert {[$new_replica ttl "ttl:10"] > 0}
            assert_equal [$new_replica ttl "key:0"] {-1}
        }

        test {UPGRADE STATUS shows done} {
            set status [$new_replica upgrade status]
            assert_equal [dict get $status state] "done"
            assert {[dict get $status keys_transferred] >= 71}
        }
    }
}
