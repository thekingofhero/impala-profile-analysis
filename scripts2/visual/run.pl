#!/usr/bin/perl

@ARGV = ('-s', '-1', '-l', 'C:\Development\IntelCoFluentStudio_v5.0.1\workspace\Impala\ImpalaFrameworkDebug\cofs_execution.log');

if (!@ARGV){
	die "Usage: run.pl -s <ServerID> -l <LogFile>"
}

use Getopt::Long;
$SID = -1;
$maxSID = -1;
@Servers = ();

GetOptions("s=i" => \$SID, "l=s" => \$log);
open(MYLOG,$log) or die "can't open log file $log!";

$lcnt = 0;

if ($SID == -1) {
	while(<MYLOG>){
		$line = $_;
		if($line=~/\[INFO\] (?<cycle>.*) ns:.*ImpalaServer\[(?<serverid>\d+)\]/){
			next if ($maxSID > $+{serverid});
			$maxSID = $+{serverid};
			$lcnt++;
			last if ($lcnt >=1000);
		}	
	}
	$serverCnt = $maxSID + 1;
	print "Server Count: $serverCnt \n";
	exit if ($serverCnt == 0);
	for ($findex = 0; $findex <= $maxSID; $findex++)
	{
		@Servers[$findex] = $findex;
	}
} 
else
{
	@Servers[0] = $SID;
}

mkdir("output") unless(-d "output" );

foreach $i (@Servers){
	$scmd = "scan.pl -s $i $log > output/scan$i.csv";
	$sortcmd = "sort.pl -s $i $log > output/sort$i.csv";
	$disk_cmd = "disk.pl -s $i $log > output/disk$i.csv";
	$jcmd = "join.pl -s $i $log > output/join$i.csv";
	$ecmd = "exchange.pl -s $i $log > output/exchange$i.csv";
	$acmd = "aggregation.pl -s $i $log > output/aggregation$i.csv";
	$plotcmd = "gnuplot -e \"serverID=$i\" Impala.plt";

	system ($disk_cmd);
	system ($scmd);
	system ($sortcmd);
	system ($jcmd);
	system ($ecmd);
	system ($acmd);
	system ($plotcmd);
}
close(MYLOG);