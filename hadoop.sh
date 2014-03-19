#!/bin/bash
log_CAP=$(date -d "-1 hours" +"%Y%m%d-%H")
idc111_log_path=/data/capturelog/idc111/old
idc222_log_path=/data/capturelog/idc222/old
py_path=/data/hadoop/bin
hadoop=/data/hadoop/bin/hadoop
python=`which python`
stream=/data/hadoop/contrib/streaming/hadoop-streaming-1.2.1.jar
mapper="dcmap.py"
reducer="dc.py"

echo $log_CAP
$hadoop dfs -copyFromLocal $idc111_log_path/capture-${log_CAP}.log /input/capture-${log_CAP}.idc111.log
$hadoop dfs -copyFromLocal $idc222_log_path/capture-${log_CAP}.log /input/capture-${log_CAP}.idc222.log
function run_job()
{
$hadoop jar $stream \
-file $mapper \
-file $reducer \
-mapper "python dcmap.py" \
-reducer "python dc.py" \
-input /input/capture-${log_CAP}.*.log \
-output /output/capture-${log_CAP} 
}
run_job
$hadoop dfs -copyToLocal /output/capture-${log_CAP}/part-00000  /data/fenxi/capture-${log_CAP}.an
$hadoop dfs -rmr /input/capture-${log_CAP}.*.log 
$hadoop dfs -rmr /output/capture-${log_CAP}
/bin/sed  -i '/-/d' /data/fenxi/capture-${log_CAP}.an 
/bin/cat /data/fenxi/capture-${log_CAP}.an |/bin/sort -k2 -n -r > /data/fenxi/capture-${log_CAP}.an.sort
/bin/rm -rf /data/fenxi/capture-${log_CAP}.an 
