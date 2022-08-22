set -x

HADOOP_STREAMING_JAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming.jar
HDFS_INPUT_DIR=$1
HDFS_OUTPUT_DIR=$2
JOB_NAME=$3

hdfs dfs -rm -r -skipTrash $HDFS_OUTPUT_DIR

yarn jar $HADOOP_STREAMING_JAR \
        -D mapreduce.job.name=$JOB_NAME \
	-D mapred.reduce.tasks=3 \
        -files mapper.py,reducer.py \
        -mapper 'python3 mapper.py' \
        -reducer 'python3 reducer.py' \
        -input $HDFS_INPUT_DIR \
        -output $HDFS_OUTPUT_DIR

hdfs dfs -cat $HDFS_OUTPUT_DIR/part-00000 | head -n 50 | tee hw02_mr_data_ids.out
