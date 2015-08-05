#! /usr/bin/perl
use autodie;


if (@ARGV !=7)
{
	die "Usage: metadata_generator.sh [database name(tpcds_text_3000)][file format(TEXT/PARQUET/PARQUET] [compression code(NONE/NONE/SNAPPY)] [table type(HDFS)] [impala ip(tracing024)] [impala port(21000)] [text database to compare(tpcds_text_3000)]"
}


# order of the argv: database_name file_format compression_code table_type impala_ip impala_port

my $database=@ARGV[0];
my $file_format=@ARGV[1];
my $compression_code=@ARGV[2];
my $table_type=@ARGV[3];
my $impala_ip=@ARGV[4];
my $impala_port=@ARGV[5];
my $database_size=@ARGV[6];

my @text_size_list = `./text_size.pl $database_size`;

my @lines = `impala-shell -i "$impala_ip:$impala_port" -d $database -q "show tables"`;

$file_format=~ tr/[A-Z]/[a-z]/;
$compression_code=~ tr/[A-Z]/[a-z]/;
$table_type=~ tr/[A-Z]/[a-z]/;


$outfilename='>metadata_'.$file_format.'_'.$compression_code.'.xml';
if( !open METAOUT , $outfilename)
{
	die "cannot create output file:$!";
}

select METAOUT;

$file_format=~ tr/[a-z]/[A-Z]/;
$compression_code=~ tr/[a-z]/[A-Z]/;
$table_type=~ tr/[a-z]/[A-Z]/;

printf "<TableDescriptors>\n";


$tableID=0;
foreach $line ( @lines) 
{
	$line=~ s/\s//g;
	if($line=~s/\|(.*)\|// && $1 ne "name")
	{
		my $table_name = $1;
        
		my @table_format_list= `impala-shell -i "$impala_ip:$impala_port" -d $database -q "show table stats $table_name"`;

		my $table_format=$table_format_list[3];
		$table_format=~ s/\s//g;

		$table_format=~s/\|.*\|.*\|.*\|.*\|(.*)\|//g;
		$table_format = $1;
		my @table_stats_1 = `impala-shell -i "$impala_ip:$impala_port" -d $database -q "desc formatted $table_name"`;
		
		my $external_table;
		foreach $line (@table_stats_1)
		{
			$line =~ s/\s//g;
			if($line=~s/\|(.*)\|(.*)\|(.*)\|// && $1 eq "TableType:")
			{
				$external_table = $2;
			}

		}
		
		$file_format_copy = $file_format;	
		if($file_format_copy=~ s/$table_format//i && $external_table ne "EXTERNAL_TABLE")
		{
		
		printf "\t<TableDescriptor>\n";	
		$tableID=$tableID+1;

		my @column_stats = `impala-shell -i "$impala_ip:$impala_port" -d $database -q "show column stats $table_name"`;
		my @table_stats = `impala-shell -i "$impala_ip:$impala_port" -d $database -q "desc formatted $table_name"`;
		
		local $number_cluster_columns=0;
		my $table_size;
        my $text_size;#text table size
		my $num_column;
		my $total_row_num;
		
		 foreach $line (@table_stats)
		 {
             $line =~ s/\s//g;
             if($line=~s/\|\|(.*)\|(.*)\|//)
			 { 
			 	if($1 eq "totalSize")  { $table_size=$2;}
				#else if ($1 == "")   { $num_column = $2;}
				if ($1 eq "numRows")   { $total_row_num = $2;}
				#else if () {}
			}

			 if($line=~s/\|(.+)\|(.*)\|(.*)\|// && $1 eq "#PartitionInformation")
			 {
			    
				foreach $line (@table_stats)
				{
					 $line =~ s/\s//g;
					if($line=~s/\|(.+)\|(.*)\|(.*)\|// && $1 ne "#col_name" && $1 ne "#DetailedTableInformation")
					{
						$number_cluster_columns= $number_cluster_columns +1;
					}
					if ($1 eq "#DetailedTableInformation") {last;}
											   
				}
				
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

			$temp = $table_format_list[-2];
			$temp =~ s/\s//g;

			if($temp =~ s/\|Total\|(\d)\|\d\|\.*\|.*\|\| //)
			{
				$total_row_num = $1;
			}
        }
	my $table_prefix = $1;

       if( $table_name =~ /(.*)_$file_format/i)
	{
         $table_prefix = $1;
	}
	else 
{
$table_prefix = $table_name;
}


        foreach $text_ts (@text_size_list)
        {
            if($text_ts =~ /($table_prefix)\t(.*)/)
           {
               $text_size = $2;
           }
         }
if ($table_size!=0)
{
         $compression_ratio = $text_size / $table_size;
}
else 
{
$compression_ratio =0;
}
#$compression_ratio=1;

		printf "\t\t<TableId>$tableID</TableId>\n\t\t<DatabaseName>$database</DatabaseName>\n\t\t<TableName>$table_name</TableName>\n\t\t<TableSize>$table_size</TableSize>\n\t\t<NumberClusteringColumns>$number_cluster_columns</NumberClusteringColumns>\n\t\t<TotalNumberRows>$total_row_num</TotalNumberRows>\n\t\t<TableType>$table_type</TableType>\n\t\t<FileFormat>$file_format</FileFormat>\n\t\t<CompressionCodec>$compression_code</CompressionCodec>\n\t\t<CompressionRatio>$compression_ratio</CompressionRatio>\n";


		my $columnID=0;
		

		printf "\t\t<ColumnStats>\n";
		foreach $line (@column_stats) 
		{
			$line =~ s/\s//g;
			if($line=~s/\|(.*)\|(.*)\|(.*)\|(.*)\|(.*)\|(.*)\|// && $1 ne "Column")
			{
		 		$columnID=$columnID+1;
				my $column_name = $1;
				my $column_type = $2;
				my $distinct = $3;
				printf "\t\t\t<Column column_id=\"$columnID\" name=\"$column_name\" type=\"$column_type\" distinct=\"$distinct\" />\n";
			}
		}
		printf "\t\t</ColumnStats>\n";
		$num_column = $columnID;

		printf "\t\t<NumberColumns>$num_column</NumberColumns>\n\t</TableDescriptor>\n";
		}
	}
}

printf "</TableDescriptors>\n";

select STDOUT;


