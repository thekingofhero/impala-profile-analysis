#! /usr/bin/perl
use autodie;


if (@ARGV !=1)
{
	die "Usage: text_size.pl [database size(3000)]"
}



my $database=@ARGV[0];
my @lines = `impala-shell -i "tracing024:21000" -d $database -q "show tables"`;

$outfilename='>text_size.txt';
if( !open METAOUT , $outfilename)
{
	die "cannot create output file:$!";
}

#select METAOUT;

foreach $line ( @lines) 
{
	$line=~ s/\s//g;
	if($line=~s/\|(.*)\|// && $1 ne "name")
	{
		my $table_name = $1;

		my @table_stats_1 = `impala-shell -i "tracing024:21000" -d $database -q "desc formatted $table_name"`;
		
		my $external_table;
		foreach $line (@table_stats_1)
		{
			$line =~ s/\s//g;
			if($line=~s/\|(.*)\|(.*)\|(.*)\|// && $1 eq "TableType:")
			{
				$external_table = $2;
			}

		}
		
		if($external_table ne "EXTERNAL_TABLE")
		{
		
		my @table_stats = `impala-shell -i "tracing024:21000" -d $database -q "desc formatted $table_name"`;
		
		my $table_size;
		
		 foreach $line (@table_stats)
		 {
             $line =~ s/\s//g;
             if($line=~s/\|\|(.*)\|(.*)\|//)
			 { 
			 	if($1 eq "totalSize")  { $table_size=$2;}
			}

		}


		 if($table_name =~ /store_sales_/s)
         {
            my @store_sales_part = `hdfs dfs -ls /user/hive/warehouse/$database.db/$table_name/*`;
            my $one_part;
            my $index=0;
            foreach $one_part (@store_sales_part)
            {
                if ($one_part =~/(.+)\s+(.+)\s+(.+)\s+(.+)\s+(.+)\s+(.+)\s+(.+)\s+(.+)/s)
               {
                $table_size+= $5;
               }
            }

        }
        $table_name =~ s/(.*)_text//;
		printf "$1\t$table_size\n";
        }
	}
}

#select STDOUT;


