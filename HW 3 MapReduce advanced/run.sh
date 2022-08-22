set -x

HADOOP_STREAMING_JAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming.jar
HDFS_INPUT_DIR=$1
HDFS_OUTPUT_DIR=$2
JOB_NAME=$3
NUM_REDUCERS=3

hdfs dfs -rm -r -skipTrash ${HDFS_OUTPUT_DIR}_tmp
hdfs dfs -rm -r -skipTrash $HDFS_OUTPUT_DIR

( yarn jar $HADOOP_STREAMING_JAR \
        -D mapreduce.job.name=$JOB_NAME \
	-D stream.num.map.output.key.fields=2 \
	-D stream.num.reduce.output.key.fields=2 \
	-D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
	-D mapreduce.partition.keycomparator.options="-k1,1 -k2,2" \
        -files mapper.py,reducer.py \
        -mapper 'python3 mapper.py' \
        -combiner 'python3 reducer.py' \
	-reducer 'python3 reducer.py' \
        -numReduceTasks $NUM_REDUCERS \
	-input $HDFS_INPUT_DIR \
        -output ${HDFS_OUTPUT_DIR}_tmp &&

yarn jar $HADOOP_STREAMING_JAR \
	-D mapreduce.job.name=$JOB_NAME \
	-D stream.num.map.output.key.fields=3 \
	-D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
	-D mapreduce.partition.keycomparator.options="-k1,1n -k3nr" \
	-files reducer_2.py \
	-mapper cat \
	-reducer "python3 reducer_2.py" \
	-input ${HDFS_OUTPUT_DIR}_tmp \
	-output ${HDFS_OUTPUT_DIR}
) || echo "Error"

hdfs dfs -rm -r -skipTrash ${HDFS_OUTPUT_DIR}_tmp
hdfs dfs -cat $HDFS_OUTPUT_DIR/part-00000 | tee hw03_mr_advanced_output.out
