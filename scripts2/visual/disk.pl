#!/usr/bin/perl

# @ARGV = ('-s', '-1', 'C:\Development\logs\impala-sim\impala_sim_debug.log');

if (!@ARGV) {
	die "Usage: disk.pl -s <ServerID> <log file name>"
}

use Getopt::Long;
$SID = -1;

GetOptions("s=i" => \$SID);

print"TimeStamp,ServerId,DiskWorkerId,RequestContextId,DiskId,BytesRead,DiskSpeed\n";

$lcount = 0;

while(<>) {
	$line = $_;
#[DEBUG] 5.40402e+008 ns:/Impala/ImpalaCluster/ImpalaServer[2]/DiskIoMgr/DiskWorker[3]/ReadRange> Request Context (3), 
#Server Id (2) ,Disk Name (hdd3), Disk Id (3), Split Name (store_sales_parquet_10-slotid-6), Current Bytes Read (11380.234526), 
#Total Bytes Read (76795.783478), Total Length (171307.416766), Disk Speed (101582.503808 Kb/sec.), Processor Speed (0.470588)

	if($line=~/\[DEBUG\] (?<cycle>.*) ns:.*ImpalaServer\[(?<serverid>\d+)\].*DiskWorker\[(?<disk_worker>\d+)\].*Request Context \((?<req_ctx_id>\d+)\).*Disk Id \((?<disk_id>\d+)\).*Total Bytes Read \((?<bytes_read>\d+.\d+)\).*Disk Speed \((?<disk_speed>\d+.\d+).*/){
		next if (($SID!=-1) && ($SID!=$+{serverid}));		
		$timestamp = $+{cycle} * 0.000001;  # in ms
		print "$timestamp, $+{serverid}, $+{disk_worker}, $+{req_ctx_id}, $+{disk_id}, $+{bytes_read}, $+{disk_speed}\n";	
		$lcount++; 
	}	
}

if ($lcount == 0) {
	print "-1, 0, 0, 0, 0, 0, 0, 0, 0\n";
}