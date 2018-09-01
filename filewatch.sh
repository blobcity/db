#!/usr/bin/expect
set count [llength $argv]
if { $argc < 1 } {
	puts "pass dbaname as the first argument"
	exit
}
set timeout 1
set path [lindex $argv 0]

spawn telnet localhost 10113

expect "username"
send "root\r"
expect "password"
send "root\r"

expect "command1"
send "folder-watch $path\r"


expect "result2"
send "exit\r"

interact
