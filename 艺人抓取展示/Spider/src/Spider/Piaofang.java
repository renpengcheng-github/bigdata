package Spider;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.alibaba.fastjson.JSONObject;

public class Piaofang {

	public static boolean main(String[] args) throws Exception {
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
		// String time = sdf.format(dBefore); //格式化前一天

		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		// cal.add(Calendar.MONTH, -1);
		cal.set(Calendar.DAY_OF_MONTH, cal.getActualMinimum(Calendar.DAY_OF_MONTH));
		String Month_firstday = sdf.format(cal.getTime());// 例如3月运行
															// 就显示日期为20170301
		String product = "china_piaofang";
		String HiveTable_out = "/music/warehouse/artist_movie_piaofang/dt=" + Month_firstday + "/product=" + product;
		Path Hive_path = new Path(HiveTable_out);

		// String out = "/user/work/ArtistModel/"+time+"/Piaofang";
		// Path path = new Path(out);
		hdfs.delete(Hive_path, true);
		return Piaofang.runLoadMapReducue(conf, args[0], Hive_path);

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
	// common主要处理单个连续中英文，比如简介 一串英文等，不能处理逗号，但处理空格,
	// 单反斜杠\直接去掉防止转义产生的清洗串行且一定要放在最后一个来处理
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
					.replace(" / ", "").replace(" ", "").replace(",", "").replace("，", "").replace("\\u00A0", "")
					.replace("\\", "").trim());
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
	public static String getChina_piaofang_content(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("^(.*?)(:|：)(.*?)$");
			Matcher m = p.matcher(value);
			String result = null;
			while (m.find()) {
				result = m.group(3);
			}
			return result;
		} else {
			return null;
		}
	}

	public static String getTotal_piaofang(String value) {
		if (value != null) {
			String a[] = value.split(",");
			String outputs = a[a.length - 1];
			return ToolUtils.FormatNumber(outputs);
		} else {
			return null;
		}

	}

	// 不含 中文和括号
	public static String getPiaofang_firstplay(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("(^[^\u4e00-\u9fa5,(,),（,）]*)");
			Matcher m = p.matcher(value);
			String result = null;
			while (m.find()) {
				result = m.group(1);
			}
			return result;
		} else {
			return null;
		}
	}

	public static final String TAB = "\t";

	public static class PiaofangMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		// @Override
		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			if (value.toString().matches("\\{.*\\}")) {
				Text output;
				JSONObject json = JSONObject.parseObject(value.toString());
				String site_name = isKong(json.getString("site_name")) ? "null"
						: getCommon(json.getString("site_name"));

				String comment_counts = isKong(json.getString("comment_counts")) ? "null"
						: getCount(json.getString("comment_counts"));
				//
				String step_count = isKong(json.getString("step_count")) ? "null"
						: getCount(json.getString("step_count"));
				//
				String mv_type = isKong(json.getString("mv_type")) ? "null"
						: getChina_piaofang_content(getCommon(json.getString("mv_type")));
				//
				String language = isKong(json.getString("language")) ? "null" : getCommon(json.getString("language"));
				//
				String area = isKong(json.getString("area")) ? "null"
						: getChina_piaofang_content(getCommon(json.getString("area")));
				//
				String masterstar = isKong(json.getString("masterstar")) ? "null"
						: getName(json.getString("masterstar"));
				//
				String play_amount = isKong(json.getString("play_amount")) ? "null"
						: getCommon(json.getString("play_amount"));
				String today_piaofang = isKong(json.getString("today_piaofang")) ? "null"
						: getTotal_piaofang(getCommon(json.getString("today_piaofang")));

				String introduce = isKong(json.getString("introduce")) ? "null"
						: getCommon(json.getString("introduce"));

				String score_counts = isKong(json.getString("score_counts")) ? "null"
						: getCount(json.getString("score_counts"));

				String vote_counts = isKong(json.getString("vote_counts")) ? "null"
						: getCount(json.getString("vote_counts"));

				String evalution_counts = isKong(json.getString("evalution_counts")) ? "null"
						: getCount(json.getString("evalution_counts"));

				String dist_company = isKong(json.getString("dist_company")) ? "null"
						: getName(json.getString("dist_company"));

				String director = isKong(json.getString("director")) ? "null" : getName(json.getString("director"));

				String first_play_time = isKong(json.getString("first_play_time")) ? "null"
						: getPiaofang_firstplay(
								getChina_piaofang_content(getCommon(json.getString("first_play_time"))));

				String score_level = isKong(json.getString("score_level")) ? "null"
						: getCommon(json.getString("score_level"));

				String duration = isKong(json.getString("duration")) ? "null"
						: getChina_piaofang_content(getCommon(json.getString("duration")));

				String total_piaofang = isKong(json.getString("total_piaofang")) ? "null"
						: getTotal_piaofang(getCommon(json.getString("total_piaofang")));

				String fav_counts = isKong(json.getString("fav_counts")) ? "null"
						: getCount(json.getString("fav_counts"));

				String mv_title = isKong(json.getString("mv_title")) ? "null" : getName(json.getString("mv_title"));

				output = new Text(site_name + TAB + comment_counts + TAB + step_count + TAB + mv_type + TAB + language
						+ TAB + area + TAB + masterstar + TAB + play_amount + TAB + today_piaofang + TAB + introduce
						+ TAB + score_counts + TAB + vote_counts + TAB + evalution_counts + TAB + dist_company + TAB
						+ director + TAB + first_play_time + TAB + score_level + TAB + duration + TAB + total_piaofang
						+ TAB + fav_counts + TAB + mv_title);
				context.write(output, NullWritable.get());
			}
		}

	}

	// 票房添加reduce防止数据重复
	private static class Piaofang_Reduce extends Reducer<Text, NullWritable, Text, NullWritable> {
		protected void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(Piaofang.class);
		job.setJobName("Piaofang");
		job.setNumReduceTasks(1);
		job.setMapperClass(PiaofangMapper.class);
		job.setReducerClass(Piaofang_Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		return job.waitForCompletion(true);
	}

}
