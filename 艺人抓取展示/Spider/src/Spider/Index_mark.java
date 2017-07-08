package Spider;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.alibaba.fastjson.JSONObject;

public class Index_mark {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		if (args.length == 0) {
			System.err.println("Usage: rcfile <in>");
			System.exit(1);
		}
		String queue = "mapred";
		if (args.length > 1) {
			queue = args[1].matches("hql|dstream|mapred|udw|user|common") ? args[1] : "mapred";
		}
		conf.set("mapreduce.job.queuename", queue);

		FileSystem hdfs = FileSystem.get(conf);

		Date dNow = new Date();
		Date dBefore = new Date();
		Calendar calendar = Calendar.getInstance(); // 得到日历
		calendar.setTime(dNow);// 把当前时间赋给日历
		calendar.add(Calendar.DAY_OF_MONTH, -1); // 设置为前一天
		dBefore = calendar.getTime(); // 得到前一天的时间

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd"); // 设置时间格式
		String time = sdf.format(dBefore); // 格式化前一天

		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		// cal.add(Calendar.MONTH, -1);
		cal.set(Calendar.DAY_OF_MONTH, cal.getActualMinimum(Calendar.DAY_OF_MONTH));
		String Month_firstday = sdf.format(cal.getTime());// 例如3月运行
															// 就显示日期为20170301
		String HiveTable_out = "/music/warehouse/artist_index_mark/event_day=" + Month_firstday; 
		// 微指数分区只有event_day
		Path Hive_path = new Path(HiveTable_out);

		// String out = "/user/work/ArtistModel/"+time+"/Piaofang";
		// Path path = new Path(out);
		hdfs.delete(Hive_path, true);
		Index_mark.runLoadMapReducue(conf, args[0], Hive_path);

	}

	// 判断数据是否为空
	public static boolean isKong(String value) {
		if (value == null || value.matches("\\[\"\"\\]")) {
			return true;
		} else {
			return false;
		}

	}

	// 需要添加特殊符号 例如\u00A0为javastrip 里的不间断空格符 需要直接转义来消除
	// common主要处理单个连续中英文，比如简介 一串英文等，不能处理逗号，但处理空格, 单反斜杠\直接去掉防止转义产生的清洗串行且一定要放在最后一个来处理
	public static String getCommon(String value) {
		if (value != null) {
			return value.replace("\"", "").replace("[", "").replace("]", "").replace("\n", "").replace("\r", "")
					.replace("\\t", "").replace("\\n", "").replace("\\r", "").replace("//", "").replace(" / ", "")
					.replace(" ", "").replace("\\u00A0", "").replace("\\", "").trim();
		} else {
			return null;
		}

	}

	// count主要处理逗号（去除999,999 带逗号的 或中文逗号） 空格符等数字 同时处理带万的字符, 单反斜杠\直接去掉防止转义产生的清洗串行
	public static String getCount(String value) {
		if (value != null) {
			return ToolUtils.FormatNumber(value.replace("\"", "").replace("[", "").replace("]", "").replace("\n", "")
					.replace("\\t", "").replace("\r", "").replace("\\n", "").replace("\\r", "").replace("//", "")
					.replace(" / ", "").replace(" ", "").replace(",", "").replace("，", "")
					.replace("\\u00A0", "").replace("\\", "").trim());
		} else {
			return null;
		}

	}

	// name 不能处理空格，逗号，单反斜杠\ 处理为逗号以分隔
	public static String getName(String value) {
		if (value != null) {
			return value.replace("\"", "").replace("[", "").replace("]", "").replace("\n", "").replace("\r", "")
					.replace("\\t", "").replace("\\n", "").replace("\\r", "").replace("//", "").replace(" / ", "")
					.replace("\\u00A0", "").replace("\\", ",").trim();
		} else {
			return null;
		}

	}

	// 以上为通用函数
	// -------------------------------------------------------------------------------------------------------------------------------------------------
	// 以下为自定义函数
	public static final String TAB = "\001";

	public static class IndexMarkMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		// @Override
		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			List<String> list = new ArrayList<String>();
			if (value.toString().matches("\\{.*\\}")) {
				Text output;
				JSONObject json = JSONObject.parseObject(value.toString());
				String site_name = isKong(json.getString("site_name")) ? "null"
						: getCommon(json.getString("site_name"));

				String artist_title = isKong(json.getString("artist_title")) ? "null"
						: getName(json.getString("artist_title"));
				//
				String mobile_zhishu = isKong(json.getString("mobile_zhishu")) ? "null"
						: getCommon(json.getString("mobile_zhishu"));
				//
				String find_name = isKong(json.getString("find_name")) ? "null" : getName(json.getString("find_name"));
				//
				String total_zhihu = isKong(json.getString("total_zhihu")) ? "null"
						: getCommon(json.getString("total_zhihu"));
				//
				String pc_zhishu = isKong(json.getString("pc_zhishu")) ? "null"
						: getCommon(json.getString("pc_zhishu"));
				if (artist_title.trim().isEmpty() || artist_title == "null"||artist_title==null) {
					artist_title = find_name;
				}
				//pc
				String[] arraypc = pc_zhishu.split(",");
				List<String> pclist = new ArrayList<String>();
				for (int i = 0; i < arraypc.length; i++) {
					pclist.add(i, arraypc[i]);
					}
				if(arraypc.length<60){
					for (int i = arraypc.length; i < 60; i++) {
						pclist.add(i, "null");
						}
				}
				//mobile
				String[] arraymobile = mobile_zhishu.split(",");
				List<String> mobilelist = new ArrayList<String>();
				for (int i = 0; i < arraymobile.length; i++) {
					mobilelist.add(i, arraymobile[i]);
					}
				if(arraymobile.length<60){
					for (int i = arraymobile.length; i < 60; i++) {
						mobilelist.add(i, "null");
						}
				}
				//total
				String[] arraytotal = total_zhihu.split(",");
				List<String> totallist = new ArrayList<String>();
				for (int i = 0; i < arraytotal.length; i++) {
					totallist.add(i, arraytotal[i]);
					}
				if(arraytotal.length<60){
					for (int i = arraytotal.length; i < 60; i++) {
						totallist.add(i, "null");
						}
				}
				for (int i = 1; i < arraypc.length; i = i + 2) {

					String time = getCommon(pclist.get(i-1));
					String pc = getCount(pclist.get(i-1));
					String mobile = getCount(mobilelist.get(i-1));
					String total = getCount(totallist.get(i-1));
					String str = time + "|" + pc + "|" + mobile + "|" + total;
					list.add(str);
				}
				String zhishu_time = getCommon(list.toString());
				String id="null";
				String artist_id="null";
				String artist_url="null";
				String artist_img="null";
				String ctime="null";
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd"); // 设置时间格式
				Calendar cal = Calendar.getInstance();
				cal.setTime(new Date());
				cal.add(Calendar.MONTH, -1);
				cal.set(Calendar.DAY_OF_MONTH, cal.getActualMinimum(Calendar.DAY_OF_MONTH));
				String last_month_firstday = sdf.format(cal.getTime());// 例如4月运行就显示日期为20170301  来获取上月运行的时间  ，用来计算环比
				output = new Text(id + TAB + artist_id + TAB + site_name + TAB + artist_title + TAB + artist_url + TAB
						+artist_img+ TAB + zhishu_time + TAB + total_zhihu + TAB + pc_zhishu + TAB + mobile_zhishu + TAB
						+ ctime + TAB + last_month_firstday);
				/*
				id                  	string              	null               
				artist_id           	string              	null                
				site_id             	string              	site_name             
				artist_title        	string              	artist_title                 
				artist_url          	string              	null              
				artist_img          	string              	null                
				zhishu_time         	string              	zhishu_time               
				total_zhishu        	string              	total_zhihu              
				pc_zhishu           	string              	pc_zhishu              
				mobile_zhishu       	string              	mobile_zhishu            
				ctime               	string              	null               
				utime  					string					last_month_firstday
				*/
				context.write(output, NullWritable.get());
			}
		}

	}

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(Index_mark.class);
		job.setJobName("weizhishu");
		job.setNumReduceTasks(0);
		job.setMapperClass(IndexMarkMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		return job.waitForCompletion(true);
	}

}
