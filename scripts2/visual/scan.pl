#!/usr/bin/perl

#@ARGV = ('-s', '-1', 'C:\Development\logs\impala-sim\impala_sim_debug.log');

if (!@ARGV) {
	die "Usage: scan.pl -s <ServerID> <log file name>"
}

use Getopt::Long;
$SID = -1;

GetOptions("s=i" => \$SID);

print"TimeStamp,ServerId,NodeId\n";

$lcount = 0;

while(<>) {
	$line = $_;
# [INFO]  6.08107e+007 ns:/Impala/ImpalaCluster/ImpalaServer[2]/QueryExecutor/Exec> [Backend 192.168.0.2] [HdfsScanExecNode 2] has processed [current:365,total:365,expected:365(365)], 
# remaining row batches to process 0
	if($line=~/\[INFO\] (?<cycle>.*) ns:.*\[Backend 192.168.0.(?<serverid>\d+)\].*\[HdfsScanExecNode (?<nodeid>\d+)\] has processed.*/){
		#print $_;
		next if (($SID!=-1) && ($SID!=$+{serverid}));
		$timestamp = $+{cycle} * 0.000001;  # in ms
		print "$timestamp, $+{serverid}, $+{nodeid}\n";	
		$lcount++; 
	}	
}

if ($lcount == 0) {
	print "-1, 0, 0\n";
}
