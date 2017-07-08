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
		"$path"sqoop import -D mapreduce.job.queuename=common \
		--connect jdbc:mysql://192.168.217.22:9306/music_artist \
		--driver com.mysql.jdbc.Driver --username root --password 'apiapiapi' \
		--query "insert overwrite table music_artistmodel_tv partition (dt=20170301,product='aiqiyi') \
		select 'aiqiyi_tv' as site_name,tv_title as tv_name,tv_series as tv_counts,'' as update_counts,\
		'' as publish_time,first_play_time as firstplay_time,score as score_level,'' as score_counts,\
		masterstar as primary_stars,director,\
		case when tv_type='' or tv_type='null' then tv_tag\
		else tv_type end as style_classify,\
		region as area,play_amount as play_counts,'' comment_counts,\
		'' fav_counts,'' step_counts,introduce,screenwriter,language from artist_tv_play where site_id='1' \
		and \$CONDITIONS" \
		--split-by site_id \
		--fields-terminated-by '\001' \
		--delete-target-dir \
		--target-dir /music/warehouse/music_artistmodel_tv/product=aiqiyi/dt=20170301 \
		--null-string '\\N' --null-non-string '\\N' -m 1 --direct
		if [[ $? -eq 0 ]];then
			bMV=1
			hive -e "alter table music_artistmodel_tv add partition(dt=20170301,product='aiqiyi');"
		else
			echo [$date]": sqoop任务执行失败，5min后重试"
			sleep 300
		fi
	fi
	if [[ bMV -eq 0 ]];then
		"$path"
		
sqoop import -D mapreduce.job.queuename=common \
--connect jdbc:mysql://192.168.217.22:9306/music_artist \
--driver com.mysql.jdbc.Driver --username root --password 'apiapiapi' \
--query "select 'tencent_tv' as site_name,tv_title as tv_name,tv_series as tv_counts,'' as update_counts, \
'' as publish_time,first_play_time as firstplay_time,score as score_level,'' as score_counts, \
masterstar as primary_stars,director, \
case when tv_type='' or tv_type='null' then tv_tag \
else tv_type end as style_classify, \
region as area,play_amount as play_counts,'' comment_counts, \
'' fav_counts,'' step_counts,introduce,screenwriter,language from artist_tv_play where site_id='2' \
and \$CONDITIONS" \
--split-by site_id \
--fields-terminated-by '\001' \
--delete-target-dir \
--target-dir /music/warehouse/music_artistmodel_tv/product=qq/dt=20170301 \
--null-string '\\N' --null-non-string '\\N' -m 1 --direct




		if [[ $? -eq 0 ]];then
			bMV=1
			hive -e "alter table music_artistmodel_tv add partition(dt=20170301,product='qq');"
		else
			echo [$date]": sqoop任务执行失败，5min后重试"
			sleep 300
		fi
	fi
		if [[ bMV -eq 0 ]];then
		"$path"
		
sqoop import -D mapreduce.job.queuename=common \
--connect jdbc:mysql://192.168.217.22:9306/music_artist \
--driver com.mysql.jdbc.Driver --username root --password 'apiapiapi' \
--query "select 'aiqiyi_movie' as site_name,mv_title as movie_name,first_play_time as publish_time, \
masterstar as primary_stars,director,'' area,mv_tag as style_classify,language,play_amount as play_counts, \
'' as comment_counts,introduce,'' as fav_counts,'' as step_counts,score as score_level,'' as score_counts \
,'' as fans_counts,'' as index1,'' as duration,'' as evaluation_counts,'' as vote_counts from artist_mv_play \
where site_id='1' and \$CONDITIONS" \
--split-by site_id \
--fields-terminated-by '\001' \
--delete-target-dir \
--target-dir /music/warehouse/music_artistmodel_movie/product=aiqiyi/dt=20170301 \
--null-string '\\N' --null-non-string '\\N' -m 1 --direct


		if [[ $? -eq 0 ]];then
			bMV=1
			hive -e "alter table music_artistmodel_movie add partition(dt=20170301,product='aiqiyi');"
		else
			echo [$date]": sqoop任务执行失败，5min后重试"
			sleep 300
		fi
	fi
		if [[ bMV -eq 0 ]];then
		"$path"
		
sqoop import -D mapreduce.job.queuename=common \
--connect jdbc:mysql://192.168.217.22:9306/music_artist \
--driver com.mysql.jdbc.Driver --username root --password 'apiapiapi' \
--query "select 'aiqiyi_movie' as site_name,'' comment_counts,'' as  score_counts,periods as update_time \
,play_amount as play_counts,follow_acount as fans_counts,introduce as show_introduce,vv_title as show_name, \
vv_tag as style_classify,emcee as host,'' as ares,score as score_level,periods as first_show, \
'' tv_station,'' fav_counts \
from artist_variety where site_id='1' and \$CONDITIONS" \
--split-by site_id \
--fields-terminated-by '\001' \
--delete-target-dir \
--target-dir /music/warehouse/music_artistmodel_media/product=aiqiyi/dt=20170301 \
--null-string '\\N' --null-non-string '\\N' -m 1 --direct


		if [[ $? -eq 0 ]];then
			bMV=1
			hive -e "alter table music_artistmodel_media add partition(dt=20170301,product='aiqiyi');"
		else
			echo [$date]": sqoop任务执行失败，5min后重试"
			sleep 300
		fi
	fi
done



sqoop import -D mapreduce.job.queuename=common \
--connect jdbc:mysql://192.168.217.22:9306/music_artist \
--driver com.mysql.jdbc.Driver --username root --password 'apiapiapi' \
--query "select case when site_id='4' then 'yongle_concert' \
when site_id='5' then 'damai_concert' end as site_name,concert_price,performer, \
ship_city,concert_place,concert_title,concert_time,'' as fav_acount \
from artist_concert where (site_id='4' or site_id='5') and \$CONDITIONS" \
--split-by site_id \
--fields-terminated-by '\t' \
--delete-target-dir \
--target-dir /music/warehouse/artist_concert/dt=20170301/product=china_piaofang/ \
--null-string '\\N' --null-non-string '\\N' -m 1 --direct

site_name           	string              	                    
concert_price       	string              	                    
performer           	string              	                    
ship_city           	string              	                    
concert_place       	string              	                    
concert_title       	string              	                    
concert_time        	string              	                    
fav_acount          	string     



艺人库匹配表导出
sqoop import -D mapreduce.job.queuename=common \
--connect jdbc:mysql://192.168.217.22:9306/music_artist \
--driver com.mysql.jdbc.Driver --username root --password 'apiapiapi' \
--query "select id,platform,artistname,remark  \
from artist_isdisplay where \$CONDITIONS;"  \
--split-by id  \
--fields-terminated-by '\t' \
--delete-target-dir \
--target-dir /music/warehouse/music_artistmodel_artist_isdiplay/event_day=20170301 \
--null-string '\\N' --null-non-string '\\N' -m 1 --direct


艺人演出价值表导出
sqoop import -D mapreduce.job.queuename=common \
--connect jdbc:mysql://192.168.217.22:9306/music_artist \
--driver com.mysql.jdbc.Driver --username root --password 'apiapiapi' \
--query "select *  \
from music_artistmodel_concert_value where \$CONDITIONS;"  \
--split-by singer  \
--fields-terminated-by '\t' \
--delete-target-dir \
--target-dir /music/warehouse/music_artistmodel_concert_value \
--null-string '\\N' --null-non-string '\\N' -m 1 --direct