#!/usr/bin/perl

# @ARGV = ('-s', '-1', 'C:\Development\logs\impala-sim\impala_sim_debug.log');

if (!@ARGV) {
	die "Usage: sort.pl -s <ServerID> <log file name>"
}

use Getopt::Long;
$SID = -1;

GetOptions("s=i" => \$SID);

print"TimeStamp,ServerId,SortNodeId\n";

$lcount = 0;

while(<>) {
	$line = $_;
# [INFO]  1.06306e+010 ns:/Impala/ImpalaCluster/ImpalaServer[2]/QueryExecutor/Exec> [Backend 192.168.0.2] SortExecNode [8] finish execution
	if($line=~/\[INFO\] (?<cycle>.*) ns:.*\[Backend 192.168.0.(?<serverid>\d+)\].*\[SortExecNode (?<sort_id>\d+)\] finish execution/){	
		next if (($SID!=-1) && ($SID!=$+{serverid}));
		$timestamp = $+{cycle} * 0.000001;  # in ms
		print "$timestamp, $+{serverid},$+{sort_id}\n";
		$lcount++; 
	}
}

if ($lcount == 0) {
	print "-1, 0, 0\n";
}
