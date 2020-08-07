#!/bin/bash

set -x
date=$1
path=/home/disk9/musicdata/sqoop/bin/

bMV=0
bTV=0
bTieBa=0
bVariety=0
bConcert=0
bUserInfo=0
while true
do
	if [[ bMV -eq 0 ]];then
		"$path"sqoop import -D mapreduce.job.queuename=udw --connect jdbc:mysql://192.168.217.22:9306/music_artist --driver com.mysql.jdbc.Driver --username root --password 'apiapiapi' --table 'artist_mv_play' --fields-terminated-by '\001' --delete-target-dir --target-dir /music/warehouse/artist_mv_play/event_day=$date

		if [[ $? -eq 0 ]];then
			bMV=1
			hive -e "alter table artist_mv_play add partition(event_day=$date)"
		else
			echo [$date]": sqoop任务执行失败，5min后重试"
			sleep 300
		fi
	fi
: '
	if [[ bTV -eq 0 ]];then
		"$path"sqoop import -D mapreduce.job.queuename=udw --connect jdbc:mysql://192.168.217.22:9306/music_artist --driver com.mysql.jdbc.Driver --username root --password 'apiapiapi' --table 'artist_tv_play' --fields-terminated-by '\001' --delete-target-dir --target-dir /music/warehouse/artist_tv_play/event_day=$date

		if [[ $? -eq 0 ]];then
			bTV=1
			hive -e "alter table artist_tv_play add partition(event_day=$date)"
		else
			echo [$date]": sqoop任务执行失败，5min后重试"
			sleep 300
		fi
	fi

	if [[ bVariety -eq 0 ]];then
		"$path"sqoop import -D mapreduce.job.queuename=udw --connect jdbc:mysql://192.168.217.22:9306/music_artist --driver com.mysql.jdbc.Driver --username root --password 'apiapiapi' --table 'artist_variety' --fields-terminated-by '\001' --delete-target-dir --target-dir /music/warehouse/artist_variety/event_day=$date

		if [[ $? -eq 0 ]]
		then
			bVariety=1
			hive -e "alter table artist_variety add partition(event_day=$date)"
		else
			echo [$date]": sqoop任务执行失败，5min后重试"
			sleep 300
		fi
	fi
'
	if [[ bConcert -eq 0 ]];then
		"$path"sqoop import -D mapreduce.job.queuename=udw --connect jdbc:mysql://192.168.217.22:9306/music_artist --driver com.mysql.jdbc.Driver --username root --password 'apiapiapi' --table 'artist_concert' --fields-terminated-by '\001' --delete-target-dir --target-dir /music/warehouse/artist_concert/event_day=$date

		if [[ $? -eq 0 ]]
		then
			bConcert=1
			hive -e "alter table artist_concert add partition(event_day=$date)"
		else
			echo [$date]": sqoop任务执行失败，5min后重试"
			sleep 300
		fi
	fi
: '
	if [[ bUserInfo -eq 0 ]];then
		"$path"sqoop import -D mapreduce.job.queuename=udw --connect jdbc:mysql://192.168.217.22:9306/music_artist --driver com.mysql.jdbc.Driver --username root --password 'apiapiapi' --table 'artist_user_info' --fields-terminated-by '\001' --delete-target-dir --target-dir /music/warehouse/artist_user_info/event_day=$date

		if [[ $? -eq 0 ]]
		then
			bUserInfo=1
			hive -e "alter table artist_user_info add partition(event_day=$date)"
		else
			echo [$date]": sqoop任务执行失败，5min后重试"
			sleep 300
		fi
	fi
'	
	if [[ bTieBa -eq 0 ]];then
		"$path"sqoop import -D mapreduce.job.queuename=udw --connect jdbc:mysql://192.168.217.22:9306/music_artist --driver com.mysql.jdbc.Driver --username root --password 'apiapiapi' --table 'artist_index_mark' --fields-terminated-by '\001' --delete-target-dir --target-dir /music/warehouse/artist_index_mark/event_day=$date

		if [[ $? -eq 0 ]]
		then
			bTieBa=1
			hive -e "alter table artist_index_mark add partition(event_day=$date)"
			exit 0
		else
			echo [$date]": sqoop任务执行失败，5min后重试"
			sleep 300
		fi
	fi
: '	
	if [[ bTieBa -eq 0 ]];then
		"$path"sqoop import -D mapreduce.job.queuename=udw --connect jdbc:mysql://192.168.217.22:9306/music_artist --driver com.mysql.jdbc.Driver --username root --password 'apiapiapi' --table 'artist_tieba' --fields-terminated-by '\001' --delete-target-dir --target-dir /music/warehouse/artist_tieba/event_day=$date

		if [[ $? -eq 0 ]]
		then
			bTieBa=1
			hive -e "alter table artist_tieba add partition(event_day=$date)"
			exit 0
		else
			echo [$date]": sqoop任务执行失败，5min后重试"
			sleep 300
		fi
	fi
'
done
