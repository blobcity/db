#!/usr/bin/expect
spawn telnet localhost 10113

expect "username"
send "root\r"
expect "password"
send "root\r"

expect "command1"
send "map-reduce gremner tweets TokenizerMapper IntSumReducer tweets\r"

expect "result"
