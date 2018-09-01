#!/usr/bin/expect
#FORMAT: queryCode db.table -table-creation parameters
#eg: nse.data --t in-memory --f true --rf 0

#check no. of argument, 1 is required atleast
if {$argc < 1 } {
 puts "pass db.table as first argument and other parameters required for creating table"
 exit
}


set table [lindex $argv 0]


set count $argc
set params ""
# get the table parameters
while { $count > 0 } {
 set tmp [expr $argc-$count+1]
 set a2 [lindex $argv $tmp]
 set count [expr  $count-1]
 set params "$params $a2"
}

puts "other parms are:\t $params"
puts ""

set timeout 1
spawn telnet localhost 10113

puts ""
expect "username"
send "root\r"
expect "password"
send "root\r" 
expect "command1"

puts ""
send "drop-table $table\r"
expect "result"

puts ""
send "create-table $table $params\r"
expect "result"

puts ""
send "view-table $table\r"
expect "result2"

puts ""
send "exit\r"

interact
