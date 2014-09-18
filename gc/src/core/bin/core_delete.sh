#!/bin/bash

while getopts ":d:r:e:t:" option
do
	case "$option" in
		
		d)
			if [[ $OPTARG == -* ]]
			then				
				echo "-d need an argument" 
				exit 1
			fi		
		  	rm_dir=$OPTARG
		            
		;;
				
		r)
			if [[ $OPTARG == -* ]]
			then				
				echo "-l need an argument" 
				exit 1
			fi		
		  	rm_date=$OPTARG
		            
		;;
		
		e)
			if [[ $OPTARG == -* ]]
			then				
				echo "-e need an argument" 
				exit 1
			fi		
		  	dir_env=$OPTARG
		            
		;;
			
		t)
			if [[ $OPTARG == -* ]]
			then				
				echo "-t need an argument" 
				exit 1
			fi		
		  	task=$OPTARG
		            
		;;

		?)
		
		echo "usage $0:>" 
		exit 1
		
		;;
			
	esac

done

this_dir=$(dirname $0)
base_dir=$(cd $this_dir/..; pwd)
cd $base_dir
. conf/run.conf

current_date=$(date +%Y%m%d)
[[ ! -d $RUN_ERROR_LOGS/${current_date} ]] && mkdir -p $RUN_ERROR_LOGS/${current_date}
elog=$RUN_ERROR_LOGS/${current_date}/run_error.log
olog=$RUN_ERROR_LOGS/${current_date}/run_output.log

#----------- Delete HDFS Module ------------------------
# 删除HDFS过期数据的具体删除模块
deleteHdfsModule() {
	rm_hdfs_dir=$1
	if $HADOOP_INSTALL/bin/hadoop fs -test -e $rm_hdfs_dir
	then
		$HADOOP_INSTALL/bin/hadoop fs -rm -r $rm_hdfs_dir
		if [[ $? -ne 0 ]]
		then
			echo "HDFS DELETE ERROR: remove HDFS $rm_hdfs_dir error " >>$elog
		else
			echo "HDFS DELETE SUCCESS: remove HDFS $rm_hdfs_dir success" >>$olog
		fi
	fi	
}

# 删除ETL过期数据
deleteEtlDir() {
	stat_date=$2
	ym=${stat_date:0:8}
	hh=${stat_date:8:2}
	format_stat_date=$(date -d "$ym $hh" +%Y/%m/%d/%H)
	if [[ $? -ne 0 ]]
	then
		echo "ETL DELETE Error: $2 format error..."  >>$elog
	else
		rm_etl_dir=$1/$format_stat_date
		deleteHdfsModule $rm_etl_dir
	fi
}

# 删除除ETL之外的其他hdfs目录
deleteHdfsDir() {
	rm_hdfs_dir=$1/$3/$2
	deleteHdfsModule $rm_hdfs_dir
}
#-------------------- END ----------------------------------

#------------- Delete Local Module -------------------------
deleteLocalModule() {
	
	rm_local_dir=$1
	if [[ -d $rm_local_dir ]]
	then
		rm -r $rm_local_dir
		if [[ $? -ne 0 ]]
		then
			echo "LOCAL DELETE ERROR: delete $rm_local_dir error." >>$elog
		else
			echo "LOCAL DELETE SUCCESS: delete $rm_local_dir success." >>$olog
		fi
	fi
	
}

# 删除本地过期数据
deleteLocalDir() {
	rm_local_dir=$1/$3/$2
	deleteLocalModule $rm_local_dir
}
#------------- END -------------------------------------------
if [[ $dir_env == "etl" ]]
then
	deleteEtlDir $rm_dir $rm_date
elif [[ $dir_env == "hdfs" ]]
then
	deleteHdfsDir $rm_dir $rm_date $task
elif [[ $dir_env == "local" ]]
then
	deleteLocalDir $rm_dir $rm_date $task
fi