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
	yyyy=${stat_date:0:4}
	mm=${stat_date:4:2}
	dd=${stat_date:6:2}
	hh=${stat_date:8:2}
	format_stat_date=$(date -d "$yyyy$mm$dd $hh" +%Y/%m/%d/%H)
	if [[ $? -ne 0 ]]
	then
		echo "ETL DELETE Error: $2 format error..."  >>$elog
	else
		monthList="$($HADOOP_INSTALL/bin/hadoop fs -ls $1/$yyyy 2>/dev/null | awk 'NR != 1{print $8}' | awk -F"/" '{print $7}')"
		dayList="$($HADOOP_INSTALL/bin/hadoop fs -ls $1/$yyyy/$mm 2>/dev/null | awk 'NR != 1{print $8}' | awk -F"/" '{print $8}')"
		hourList="$($HADOOP_INSTALL/bin/hadoop fs -ls $1/$yyyy/$mm/$dd 2>/dev/null | awk 'NR != 1{print $8}' | awk -F"/" '{print $9}')"
		
		if [[ $mm != 01 ]]
		then
			for m in $(seq -f %02g 01 $(( $((10#$mm)) - 1 )))
			do
				if echo $monthList | grep -q $m
				then
					deleteHdfsModule $1/$yyyy/$m
				fi	
			done
		fi
		
		if [[ $dd != 01 ]]
		then
			for d in $(seq -f %02g 01 $(( $((10#$dd)) - 1 )) )
			do
				if echo $dayList | grep -q $d
				then
					deleteHdfsModule $1/$yyyy/$mm/$d
				fi
			done
		fi
		
		for h in $(seq -f %02g 00 $hh)
		do
			if echo $hourList | grep -q $h
			then
				deleteHdfsModule $1/$yyyy/$mm/$dd/$h
			fi
		done
	fi
}

# 删除除ETL之外的其他hdfs目录
deleteHdfsDir() {	
	
	if [[ $3 == "day" ]]
	then
		stat_date=$2
		yyyy=${stat_date:0:4}
		mm=${stat_date:4:2}
		dd=${stat_date:6:2}
		dateList="$($HADOOP_INSTALL/bin/hadoop fs -ls $1/$3 2>/dev/null | awk 'NR != 1{print $8}' | awk -F"/" '{print $7}')"
		
		for m in $(seq -f %02g 01 $mm)
		do
			for d in $(seq -f %02g 01 $dd)
			do
				if echo $dateList | grep -q $yyyy$m$d
				then
					deleteHdfsModule $1/$3/$yyyy$m$d
				fi
			done
		done
	fi
	
	if [[ $3 == "hour" ]]
	then
		stat_date=$2
		yyyy=${stat_date:0:4}
		mm=${stat_date:4:2}
		dd=${stat_date:6:2}
		hh=${stat_date:8:2}
		dateList="$($HADOOP_INSTALL/bin/hadoop fs -ls $1/$3 2>/dev/null | awk 'NR != 1{print $8}' | awk -F"/" '{print $7}')"
		
		for m in $(seq -f %02g 01 $mm)
		do
			for d in $(seq -f %02g 01 $dd)
			do
				for h in $(seq -f %02g 00 $hh)
				do
					if echo $dateList | grep -q $yyyy$m$d$h
					then
						deleteHdfsModule $1/$3/$yyyy$m$d$h
					fi		
				done		
			done
		done
	fi
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
	if [[ $3 == "day" ]]
	then
		for dayDate in `ls -A $1/$3`
		do
			if (( $dayDate <= $2 ))
			then
				deleteLocalModule $1/$3/$dayDate
			fi
		done
	fi
	if [[ $3 == "hour" ]]
	then
		for hourDate in `ls -A $1/$3`
		do 
			if (( $hourDate <= $2 ))
			then
				deleteLocalModule $1/$3/$hourDate
			fi
		done
	fi
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