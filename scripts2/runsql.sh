#
#   Impala Query Execution Script
#   The purpose of this script is to run impala queries and gather profile, logs and OS metrics into a predefined location
#   The script takes three arguments as input, i.e., 
#     impala server -> the ip or server name of the impala server;
#     database name -> one of the existing database name of Impala, the query is assumed to be executed on this database;
#     query name    -> the name of the file containing the actual query file; (queries should be stored in directory QUERY_DIRECTORY)
#
#    Impala Query Directory: the location of impala queries (specified in QUERY_DIRECTORY variable)
#    Impala Log Directory: the location of impala logs, it is assumed that impala folder location are same on all nodes (specified in IMPALA_LOG_DIRECTORY variable)
#    Execution Output Directory: all outputs, i.e., logs, CVTs and profile, are stored in the location specified by the value of EXECUTION_OUTPUT_DIRECTORY variable
#
#    NOTE: The above three directory locations have to be updated within the script.
#
#
if [ $# -lt 4 ]; then
	echo "Usage: runsql.sh [impala server] [query directory] [database name] [query name] [profile|all] "
	exit 1
fi
CLIENT=tracing017
IMPALA_NODES=(tracing024 tracing025 tracing026 tracing027)

CURRENT_TIMESTAMP=`date +%s`

IMPALA_SERVER=$1
QUERY_DIRECTORY=$2
DATABASE_NAME=$3
QUERY_NAME=$4
PROFILE_ONLY="all"
if [ $# -eq 5 ]; then
	PROFILE_ONLY=$5	
fi
echo "Gather ONLY PROFILE or ALL? -> [${PROFILE_ONLY}]"

#
# Update the following variables if different locations are required
#
IMPALA_LOG_DIRECTORY="/home/shihongl/impalad/level2"
EXECUTION_OUTPUT_DIRECTORY_PREFIX="/home/junliu/logs"
CVT_LOCATION="~/mCVT"

if [ ! -d "$DIRECTORY" ]; then
	mkdir -p $QUERY_DIRECTORY
fi

EXECUTION_OUTPUT_DIRECTORY=${EXECUTION_OUTPUT_DIRECTORY_PREFIX}/${QUERY_NAME}_${CURRENT_TIMESTAMP}
if [ ! -d "$DIRECTORY" ]; then
	mkdir -p $EXECUTION_OUTPUT_DIRECTORY
fi

PROFILE_LOG_DIRECTORY=${EXECUTION_OUTPUT_DIRECTORY}/profiles
if [ ! -d "$DIRECTORY" ]; then
	mkdir -p $PROFILE_LOG_DIRECTORY
fi

CVT_LOG_DIRECTORY=${EXECUTION_OUTPUT_DIRECTORY}/cvt
if [ ! -d "$DIRECTORY" ]; then
	mkdir -p $CVT_LOG_DIRECTORY
fi

echo "database name: ${DATABASE_NAME}"
echo "query name: ${QUERY_NAME}"
echo "query directory: ${QUERY_DIRECTORY}"
echo "query log directory: ${EXECUTION_OUTPUT_DIRECTORY}"
echo "profile log directory: ${PROFILE_LOG_DIRECTORY}"
echo "cvt log directory: ${CVT_LOG_DIRECTORY}"

#
# We need to clear OS cache on all nodes within the cluster
#
sync

echo "clear OS cache on tracing017"
free && sync && echo 3 >/proc/sys/vm/drop_caches && free

echo "clear OS cache on tracing024"
ssh tracing024 "free && sync && echo 3 >/proc/sys/vm/drop_caches && free"

echo "clear OS cache on tracing025"
ssh tracing025 "free && sync && echo 3 >/proc/sys/vm/drop_caches && free"

echo "clear OS cache on tracing026"
ssh tracing026 "free && sync && echo 3 >/proc/sys/vm/drop_caches && free"

echo "clear OS cache on tracing027"
ssh tracing027 "free && sync && echo 3 >/proc/sys/vm/drop_caches && free"

echo "start executing query"

if [ "$PROFILE_ONLY" = "all" ]; then
	# launch CVT
	sh ~/mCVT/runall ~/mCVT/sleep.sh ${CVT_LOG_DIRECTORY}&
fi

impala-shell -i "${IMPALA_SERVER}:21000" -d ${DATABASE_NAME} -f $QUERY_DIRECTORY/${QUERY_NAME} &> $PROFILE_LOG_DIRECTORY/${QUERY_NAME}.log

if [ "$PROFILE_ONLY" = "all" ]; then
	# we've done here, kill CVT task
	kill -9 $(ps aux | grep --max-count=1 'sleep 600' | awk '{print $2}') 

	for impalad in ${IMPALA_NODES}
	do
		# Remove older logs 
		rm -rf $EXECUTION_OUTPUT_DIRECTORY/impalad.${impalad}	
		scp -r ${impalad}:${IMPALA_LOG_DIRECTORY}/*.impala.log.INFO.* ${CLIENT}:$EXECUTION_OUTPUT_DIRECTORY
	done
fi
 
echo "output folder location: ${EXECUTION_OUTPUT_DIRECTORY}"

EXECUTION_TIME=`cat $PROFILE_LOG_DIRECTORY/${QUERY_NAME}.log | grep Fetched`
echo $EXECUTION_TIME
