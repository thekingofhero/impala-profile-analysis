#!/usr/bin/perl

#@ARGV = ('-s', '-1', 'C:\Development\logs\impala-sim\impala_sim_debug.log');

if (!@ARGV){
	die "Usage: aggregation.pl -s <ServerID> <log file name>"
}

use Getopt::Long;
$SID = -1;

GetOptions("s=i" => \$SID);

print"TimeStamp,ServerId,NodeId\n";

$lcount = 0;

while(<>) {
	$line = $_;
	
# [INFO]  5.85027e+009 ns:/Impala/ImpalaCluster/ImpalaServer[1]/QueryExecutor/Exec> [Backend 192.168.0.1] [AggregationExecNode 7] probe phase complete
	if($line=~/\[INFO\] (?<cycle>.*) ns:.*\[Backend 192.168.0.(?<serverid>\d+)\].*\[AggregationExecNode (?<nodeid>\d+)\] probe phase complete/){
	#	print $_;
		next if (($SID!=-1) && ($SID!=$+{serverid}));
		$timestamp = $+{cycle} * 0.000001;  # in ms
		print "$timestamp, $+{serverid}, $+{nodeid}\n";
		$lcount++;
	}	
}

if ($lcount == 0) {
	print "-1, 0, 0\n";
}
