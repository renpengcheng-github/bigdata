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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.alibaba.fastjson.JSONObject;

public class Media_sp_reduce {

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
	// letv------------------------------------------------------------------------------------------------------------------------------------
	// 一：host开头不能是数字
	public static String getletvhost(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("(^[^0-9]*)");
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

	// tencent------------------------------------------------------------------------------------------------------------------------------------
	public static String getTencent_publish_time(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("(^[^\u4e00-\u9fa5]*)");
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
	// mangguo-------------------------------------------------------------------------------------------------------------------------------------------

	// all[0]:show_name
	// all[1]:publish_time
	// all[2]:guest
	public static List<String> getmangguo_name_time_guest(String value) {
		List<String> all = new ArrayList<>();
		String show_name = null;
		String publish_time = null;
		String guest = null;
		String reg = ".*(:|：).*";
		if (value != null && value.matches(reg)) {
			String a[] = value.split("：|:");
			String showname_time = a[0];
			String gusetname_time = a[1];
			// 判断首字符 如果：分隔的第一块字符不带8位数字，则全为show_name
			String regp1 = "^(.*?)([0-9]{8})(.*?)";
			if (showname_time.matches(regp1) == false) {
				show_name = showname_time;
			} else {
				Pattern p1 = Pattern.compile("^([^第]*?)([0-9]{8})(.*?)");
				Matcher m1 = p1.matcher(showname_time);
				while (m1.find()) {
					show_name = m1.group(1);
				}
			}
			Pattern p2 = Pattern.compile("^(.*?)([0-9]{8})(.*?)");
			Matcher m2 = p2.matcher(showname_time);
			while (m2.find()) {
				publish_time = ToolUtils.FormatDate(m2.group(2));
			}
			// 不带中英冒号： 中英括号（）的字符
			Pattern p3 = Pattern.compile("(^[^(:|：),(,),（,）]*)");
			Matcher m3 = p3.matcher(gusetname_time);
			while (m3.find()) {
				guest = m3.group(1);
			}
		} else if (value != null && value.matches(reg) == false) {
			show_name = value;
		} else {
			return null;
		}
		all.add(show_name); // .get(0)
		all.add(publish_time);// .get(1)
		all.add(guest);// .get(2)
		return all;
	}

	// youku------------------------------------------------------------------------------------------------------------------------------------
	// 优酷评论 特殊处理
	public static String getYoukuCommentcounts(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("\\[\"(.*)次评论\"\\]");
			Matcher m = p.matcher(value);
			String result = "";
			while (m.find()) {
				result = m.group(1);
			}
			return result.replace(",", "");
		} else {
			return null;
		}
	}

	public static String getYoukupublish_time(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("^(.*?)([0-9]{8})(.*?)");
			Matcher m = p.matcher(value);
			String result = null;
			while (m.find()) {
				result = ToolUtils.FormatDate(m.group(2));
			}
			return result;
		} else {
			return null;
		}
	}

	public static String getYoukushow_name(String value) {
		String result = null;
		if (value != null) {
			String reg = "^第(.*?)(:|：)(.*?)$";
			if (value.matches(reg)) {
				Pattern p1 = Pattern.compile(reg);
				Matcher m1 = p1.matcher(value);
				while (m1.find()) {
					result = m1.group(3);
				}
			} else {
				String reg1 = "^(.*?)([0-9]{8})(.*?)";
				if (value.matches(reg1)) {
					Pattern p = Pattern.compile(reg1);
					Matcher m = p.matcher(value);
					while (m.find()) {
						result = m.group(1) + m.group(3);
					}
				} else {
					result = value;
				}
			}
			return result;
		} else {
			return null;
		}
	}
	// souhu------------------------------------------------------------------------------------------------------------------------------------

	// 搜狐评论 特殊处理
	public static String getSouhuCommentcounts(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("\\[\"\\((.*)人参与，(.*)条评论\\)\"\\]");
			Matcher m = p.matcher(value);
			String result = "";
			while (m.find()) {
				result = m.group(2);
			}
			return result.replace(",", "");
		} else {
			return null;
		}
	}

	public static final String TAB = "\001";

	public static class MediaspMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			Text output;
			if (value.toString().matches("\\{.*\\}")) {
				JSONObject json = JSONObject.parseObject(value.toString());
				// 网站标示
				String site_name = isKong(json.getString("site_name")) ? "null"
						: getCommon(json.getString("site_name"));

				if (site_name.matches(".*letv_sp.*")) {
					String show_name = isKong(json.getString("show_name")) ? "null"
							: ToolUtils.getName_Time(ToolUtils.getName((json.getString("show_name"))));
					String publish_time = isKong(json.getString("publish_time")) ? "null"
							: getCommon(json.getString("publish_time"));
					if (publish_time == null || publish_time.trim().isEmpty() || publish_time == "null") {
						publish_time = isKong(json.getString("show_name")) ? "null"
								: ToolUtils.getTime_Name(ToolUtils.getName((json.getString("show_name"))));
					}
					String host = isKong(json.getString("host")) ? "null"
							: getletvhost(getCommon(json.getString("host")));
					String guest = isKong(json.getString("guest")) ? "null"
							: ToolUtils.getName(json.getString("guest"));
					String sp_plays = isKong(json.getString("sp_plays")) ? "null"
							: getCount(json.getString("sp_plays"));
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getCount(json.getString("comment_counts"));
					String barrage_counts = isKong(json.getString("barrage_counts")) ? "null"
							: getCount(json.getString("barrage_counts"));
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: getCount(json.getString("fav_counts"));
					String step_counts = isKong(json.getString("step_counts")) ? "null"
							: getCount(json.getString("step_counts"));
					String discuss_counts = isKong(json.getString("discuss_counts")) ? "null"
							: getCount(json.getString("discuss_counts"));
					String vote_counts = isKong(json.getString("vote_counts")) ? "null"
							: getCount(json.getString("vote_counts"));
					output = new Text(site_name + TAB + show_name + TAB + publish_time + TAB + host + TAB + guest + TAB
							+ sp_plays + TAB + comment_counts + TAB + barrage_counts + TAB + fav_counts + TAB
							+ step_counts + TAB + discuss_counts + TAB + vote_counts);

					context.write(new Text(site_name + TAB + show_name + TAB + host + TAB + guest), output);
				} else if (site_name.matches(".*mangguo_sp.*")) {
					String show_name = isKong(json.getString("show_name")) ? "null"
							: ToolUtils.getName(json.getString("show_name"));
					String publish_time = isKong(json.getString("publish_time")) ? "null"
							: getCommon(json.getString("publish_time"));
					if (publish_time == null || publish_time.trim().isEmpty() || publish_time == "null") {
						publish_time = isKong(json.getString("show_name")) ? "null"
								: getmangguo_name_time_guest(ToolUtils.getName(json.getString("show_name"))).get(1);
					}
					String host = isKong(json.getString("host")) ? "null" : ToolUtils.getName(json.getString("host"));
					String guest = isKong(json.getString("guest")) ? "null"
							: ToolUtils.getName(json.getString("guest"));
					// if (guest == null || guest.trim().isEmpty()||guest ==
					// "null") {
					// guest=isKong(json.getString("show_name"))?"null"
					// :getmangguo_name_time_guest(ToolUtils.getName(json.getString("show_name"))).get(2);
					// }
					String sp_plays = isKong(json.getString("sp_plays")) ? "null"
							: ToolUtils.FormatNumber(getCount(json.getString("sp_plays")));
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getCount(json.getString("comment_counts"));
					String barrage_counts = isKong(json.getString("barrage_counts")) ? "null"
							: getCount(json.getString("barrage_counts"));
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: ToolUtils.FormatNumber(getCount(json.getString("fav_counts")));
					String step_counts = isKong(json.getString("step_counts")) ? "null"
							: getCount(json.getString("step_counts"));
					String discuss_counts = isKong(json.getString("discuss_counts")) ? "null"
							: getCount(json.getString("discuss_counts"));
					String vote_counts = isKong(json.getString("vote_counts")) ? "null"
							: getCount(json.getString("vote_counts"));
					output = new Text(site_name + TAB + show_name + TAB + publish_time + TAB + host + TAB + guest + TAB
							+ sp_plays + TAB + comment_counts + TAB + barrage_counts + TAB + fav_counts + TAB
							+ step_counts + TAB + discuss_counts + TAB + vote_counts);

					context.write(new Text(site_name + TAB + show_name + TAB + host + TAB + guest), output);
				} else if (site_name.matches(".*tencent_sp.*")) {
					String show_name = isKong(json.getString("show_name")) ? "null"
							: ToolUtils.getName(json.getString("show_name"));
					String publish_time = isKong(json.getString("publish_time")) ? "null"
							: getTencent_publish_time(getCommon(json.getString("publish_time")));
					String host = isKong(json.getString("host")) ? "null" : ToolUtils.getName(json.getString("host"));
					String guest = isKong(json.getString("guest")) ? "null"
							: ToolUtils.getName(json.getString("guest"));
					String sp_plays = isKong(json.getString("sp_plays")) ? "null"
							: ToolUtils.FormatNumber(getCount(json.getString("sp_plays")));
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getCount(json.getString("comment_counts"));
					String barrage_counts = isKong(json.getString("barrage_counts")) ? "null"
							: getCount(json.getString("barrage_counts"));
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: ToolUtils.FormatNumber(getCount(json.getString("fav_counts")));
					String step_counts = isKong(json.getString("step_counts")) ? "null"
							: getCount(json.getString("step_counts"));
					String discuss_counts = isKong(json.getString("discuss_counts")) ? "null"
							: getCount(json.getString("discuss_counts"));
					String vote_counts = isKong(json.getString("vote_counts")) ? "null"
							: getCount(json.getString("vote_counts"));
					output = new Text(site_name + TAB + show_name + TAB + publish_time + TAB + host + TAB + guest + TAB
							+ sp_plays + TAB + comment_counts + TAB + barrage_counts + TAB + fav_counts + TAB
							+ step_counts + TAB + discuss_counts + TAB + vote_counts);
					context.write(new Text(site_name + TAB + show_name + TAB + host + TAB + guest), output);

				} else if (site_name.matches(".*souhu_sp.*")) {
					String show_name = isKong(json.getString("show_name")) ? "null"
							: ToolUtils.getName_Time(ToolUtils.getName(json.getString("show_name")).trim()
									.replace("\t", "").replace(",", ""));
					String publish_time = isKong(json.getString("publish_time")) ? "null"
							: getCommon(json.getString("publish_time"));
					if (publish_time == null || publish_time.trim().isEmpty() || publish_time == "null") {
						publish_time = isKong(json.getString("show_name")) ? "null"
								: ToolUtils.getTime_Name(ToolUtils.getName(json.getString("show_name")).trim()
										.replace("\t", "").replace(",", ""));
					}
					String host = isKong(json.getString("host")) ? "null" : ToolUtils.getName(json.getString("host"));
					String guest = isKong(json.getString("guest")) ? "null"
							: ToolUtils.getName(json.getString("guest"));
					String sp_plays = isKong(json.getString("sp_plays")) ? "null"
							: ToolUtils.FormatNumber(getCount(json.getString("sp_plays")));
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getSouhuCommentcounts(json.getString("comment_counts"));
					String barrage_counts = isKong(json.getString("barrage_counts")) ? "null"
							: getCount(json.getString("barrage_counts"));
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: ToolUtils.FormatNumber(json.getString("fav_counts"));
					String step_counts = isKong(json.getString("step_counts")) ? "null"
							: getCount(json.getString("step_counts"));
					String discuss_counts = isKong(json.getString("discuss_counts")) ? "null"
							: getCount(json.getString("discuss_counts"));
					String vote_counts = isKong(json.getString("vote_counts")) ? "null"
							: getCount(json.getString("vote_counts"));
					output = new Text(site_name + TAB + show_name + TAB + publish_time + TAB + host + TAB + guest + TAB
							+ sp_plays + TAB + comment_counts + TAB + barrage_counts + TAB + fav_counts + TAB
							+ step_counts + TAB + discuss_counts + TAB + vote_counts);
					context.write(new Text(site_name + TAB + show_name + TAB + host + TAB + guest), output);
				} else if (site_name.matches(".*youku_sp.*")) {

					String show_name = isKong(json.getString("show_name")) ? "null"
							: getYoukushow_name(ToolUtils.getName(json.getString("show_name")));
					String publish_time = isKong(json.getString("publish_time")) ? "null"
							: getYoukupublish_time(getCommon(json.getString("publish_time")));
					if (publish_time == null || publish_time.trim().isEmpty() || publish_time == "null") {
						publish_time = isKong(json.getString("show_name")) ? "null"
								: getYoukupublish_time(getCommon(json.getString("show_name")));
					}
					String host = isKong(json.getString("host")) ? "null" : ToolUtils.getName(json.getString("host"));
					String guest = isKong(json.getString("guest")) ? "null"
							: ToolUtils.getName(json.getString("guest"));
					String sp_plays = isKong(json.getString("sp_plays")) ? "null"
							: ToolUtils.FormatNumber(getCount(json.getString("sp_plays")));
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getYoukuCommentcounts(json.getString("comment_counts"));
					String barrage_counts = isKong(json.getString("barrage_counts")) ? "null"
							: getCount(json.getString("barrage_counts"));
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: ToolUtils.FormatNumber(getCount(json.getString("fav_counts")));
					String step_counts = isKong(json.getString("step_counts")) ? "null"
							: getCount(json.getString("step_counts"));
					String discuss_counts = isKong(json.getString("discuss_counts")) ? "null"
							: getCount(json.getString("discuss_counts"));
					String vote_counts = isKong(json.getString("vote_counts")) ? "null"
							: getCount(json.getString("vote_counts"));
					output = new Text(site_name + TAB + show_name + TAB + publish_time + TAB + host + TAB + guest + TAB
							+ sp_plays + TAB + comment_counts + TAB + barrage_counts + TAB + fav_counts + TAB
							+ step_counts + TAB + discuss_counts + TAB + vote_counts);
					context.write(new Text(site_name + TAB + show_name + TAB + host + TAB + guest), output);

				}

			}

		}

	}

	private static class Sp_Reduce extends Reducer<Text, Text, Text, Text> {

		// 将结果输出到多个文件或多个文件夹
		public MultipleOutputs<Text, Text> mos;

		// 创建MultipleOutputs对象
		public void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);
		}

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int max = 0;
			String str = "";
			Text output;
			for (Text value : values) {
				String[] vals = value.toString().split(TAB);
				int[] integers = { 6, 8, 9 };// 取6,8,9元素（count）之和最大的数据，理论上是取到了三者保留最全的记录
				for (int i : integers) {
					String reg = "[0-9]+";// 非数字则赋值为字符串的0
					if (vals[i].matches(reg) == false) {
						vals[i] = "0";
					}
				}
				int temp = Integer.parseInt(vals[6]) + Integer.parseInt(vals[8]) + Integer.parseInt(vals[9]);// 因为是count人数
																												// 所以转为整型变量
				if (temp >= max) // 做一个最大值替换
				{
					max = temp;
					// 赋值给str
					str = value.toString();
				}

			}
			output = new Text(str);
			String site_name = str.split(TAB)[0]; // 以site_name为多文件输出的依据
			if (site_name.equals("letv_sp")) {
				mos.write("letv", output, new Text(), "product=letv/");
			} else if (site_name.equals("mangguo_sp")) {
				mos.write("mangguo", output, new Text(), "product=mangguo/");
			} else if (site_name.equals("tencent_sp")) {
				mos.write("tencent", output, new Text(), "product=qq/");
			} else if (site_name.equals("souhu_sp")) {
				mos.write("souhu", output, new Text(), "product=souhu/");
			} else if (site_name.equals("youku_sp")) {
				mos.write("youku", output, new Text(), "product=youku/");
			}
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(Media_sp_reduce.class);
		job.setJobName("Reduce_test");
		job.setNumReduceTasks(10);
		job.setMapperClass(MediaspMapper.class);
		// 添加reduce过程
		job.setReducerClass(Sp_Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleOutputs.addNamedOutput(job, "souhu", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "youku", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "tencent", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "mangguo", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "letv", TextOutputFormat.class, Text.class, NullWritable.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		// job.waitForCompletion(true);

		/*
		 * 可以选择添加这一reduce过程 添加入下方则没有此reduce过程 job = Job.getInstance(conf);
		 * job.setJarByClass(Media_sp_reduce.class);
		 * job.setJobName("Reduce_test"); job.setNumReduceTasks(0);
		 * job.setMapperClass(MediaspMapper.class); //添加reduce过程
		 * job.setReducerClass(Sp_Reduce.class);
		 * job.setInputFormatClass(TextInputFormat.class);
		 * job.setOutputKeyClass(Text.class);
		 * job.setOutputValueClass(Text.class);
		 * MultipleOutputs.addNamedOutput(job, "youku", TextOutputFormat.class,
		 * Text.class, NullWritable.class); MultipleOutputs.addNamedOutput(job,
		 * "tencent", TextOutputFormat.class, Text.class, NullWritable.class);
		 * MultipleOutputs.addNamedOutput(job, "mangguo",
		 * TextOutputFormat.class, Text.class, NullWritable.class);
		 * MultipleOutputs.addNamedOutput(job, "letv", TextOutputFormat.class,
		 * Text.class, NullWritable.class); FileInputFormat.setInputPaths(job,
		 * input); FileOutputFormat.setOutputPath(job, output);
		 */
		return job.waitForCompletion(true);
	}

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
		
	    Calendar cal = Calendar.getInstance();
	     cal.setTime(new Date());
	     //cal.add(Calendar.MONTH, -1);
	     cal.set(Calendar.DAY_OF_MONTH, cal.getActualMinimum(Calendar.DAY_OF_MONTH));
	     String Month_firstday=sdf.format(cal.getTime());//例如3月运行 就显示日期为20170301
	     String HiveTable_out="/music/warehouse/music_artistmodel_mediasp/dt="+Month_firstday;
	     Path Hive_path = new Path(HiveTable_out);
	     
		//String time = sdf.format(dBefore); // 格式化前一天
		//String out = "/user/work/ArtistModel/" + time + "/Media_sp";
		//Path path = new Path(out);
		hdfs.delete(Hive_path, true);
		return Media_sp_reduce.runLoadMapReducue(conf, args[0], Hive_path);

	}

}
