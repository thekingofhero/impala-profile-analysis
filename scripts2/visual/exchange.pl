#!/usr/bin/perl

#@ARGV = ('-s', '-1', 'C:\Development\logs\impala-sim\impala_sim_debug.log');

if (!@ARGV) {
	die "Usage: exchange.pl -s <ServerID> <log file name>"
}

use Getopt::Long;
$SID = -1;

GetOptions("s=i" => \$SID);

print"TimeStamp,ServerId,FragID,ExNodeId,Rows\n";

$lcount = 0;

while(<>){
	$line = $_;
# [INFO]  1.16051e+010 ns:/Impala/ImpalaCluster/ImpalaServer[3]/QueryExecutor/Exec> [Backend 192.168.0.3] [Plan Fragment 4] [ExchangeExecNode 15] has finished execution with [276] rows
	if($line=~/\[INFO\] (?<cycle>.*) ns:.*\[Backend 192.168.0.(?<serverid>\d+)\].*\[Plan Fragment (?<fid>\d+)\] \[ExchangeExecNode (?<exch_id>\d+)\].*\[(?<rows>\d+)\] rows/){
		next if (($SID!=-1) && ($SID!=$+{serverid}));
		$timestamp = $+{cycle} * 0.000001;  # in ms
		print "$timestamp,$+{serverid},$+{fid},$+{exch_id},$+{rows}\n";
		$lcount++; 
	}
}

if ($lcount == 0) {
	print "-1, 0, 0, 0, 0\n";
}