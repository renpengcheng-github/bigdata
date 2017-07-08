package Spider;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
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
import Spider.ToolUtils;
import com.alibaba.fastjson.JSONObject;

public class Concert {

	public static final String TAB = "\t";

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
	public static String getsplit_name(String value) {
		if (value != null) {
			List<String> all = new ArrayList<String>();
			String a[] = value.replace(":", "").replace("：", "").split(",");
			String outputs = "";
			for (int i = 0; i < a.length; i++) {
				if (a[i].trim().isEmpty() || a[i].matches(".*艺人团体.*")) {
					continue;
				} else {
					all.add(a[i]);
				}
			}
			outputs = StringUtils.join(all, ",");
			return outputs;
		} else {
			return null;
		}

	}

	public static String getConcert_time(String value) {
		if (value != null) {
			List<String> all = new ArrayList<String>();
			String outputs = "";
			if (value.contains(",")) {
				String a[] = value.split(",");
				String reg = ".*(:|：).*";
				for (int i = 0; i < a.length; i++) {
					if (a[i].trim().isEmpty()) {
						continue;
					} else {
						if (a[i].matches(reg) && a[i].matches(".*-.*")) {
							Pattern p = Pattern.compile("(^[^\u4e00-\u9fa5,(,),（,）]*)");
							Matcher m = p.matcher(a[i]);
							String result1 = "";
							while (m.find()) {
								result1 = m.group(1);
								all.add(result1);
							}

						} else if (a[i].matches(reg) && a[i].matches(".*-.*") == false) {
							continue;
						} else {
							all.add(a[i]);
						}
					}
				}
				outputs = StringUtils.join(all, ",");
				return outputs;
			} else {
				Pattern p = Pattern.compile("(^[^\u4e00-\u9fa5,(,),（,）]*)");
				Matcher m = p.matcher(value);
				String result = "";
				while (m.find()) {
					result = m.group(1);
				}
				return result;
			}
		} else {
			return null;
		}

	}

	// -------------------------------------------------------------------------------------------------------------------------------------------
	public static class ConcertMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Text output;
			if (value.toString().matches("\\{.*\\}")) {
				JSONObject json = JSONObject.parseObject(value.toString());
				// 网站标示
				String site_name = isKong(json.getString("site_name")) ? "null"
						: getCommon(json.getString("site_name"));
				if (json.getString("site_name").matches(".*yongle_concert.*")) {
					//
					String concert_price = isKong(json.getString("concert_price")) ? "null"
							: getCommon(json.getString("concert_price"));
					//
					String performer = isKong(json.getString("performer")) ? "null"
							: getsplit_name(getName(json.getString("performer")));
					//
					String ship_city = isKong(json.getString("ship_city")) ? "null"
							: getCommon(json.getString("ship_city"));
					//
					String concert_place = isKong(json.getString("concert_place")) ? "null"
							: getName(json.getString("concert_place"));

					String warning = isKong(json.getString("warning")) ? "null" : getName(json.getString("warning"));

					//
					String concert_title = isKong(json.getString("concert_title")) ? "null"
							: getName(json.getString("concert_title"));
					//
					String concert_time = isKong(json.getString("concert_time")) ? "null"
							: getConcert_time(getCommon(json.getString("concert_time")));
					//
					String play_time = isKong(json.getString("play_time")) ? "null"
							: getCommon(json.getString("play_time"));

					String picture_url = isKong(json.getString("picture_url")) ? "null"
							: getCount(json.getString("picture_url"));
					// 永乐fav_count为null
					String fav_count = isKong(json.getString("fav_count")) ? "null"
							: getCommon(json.getString("fav_count"));

					output = new Text(site_name + TAB + concert_price + TAB + performer + TAB + ship_city + TAB
							+ concert_place + TAB + concert_title + TAB + concert_time + TAB + fav_count);
					context.write(new Text(performer  + TAB + concert_time), output);

				} else if (site_name.matches(".*damai_concert.*")) {
					String concert_price = isKong(json.getString("concert_price")) ? "null"
							: getCommon(json.getString("concert_price"));

					String performer = isKong(json.getString("performer")) ? "null"
							: getName(json.getString("performer"));
					//
					String ship_city = isKong(json.getString("ship_city")) ? "null"
							: getCommon(json.getString("ship_city"));
					//
					String concert_place = isKong(json.getString("concert_place")) ? "null"
							: getName(json.getString("concert_place"));

					String concert_title = isKong(json.getString("concert_title")) ? "null"
							: getName(json.getString("concert_title"));
					//
					String concert_time = isKong(json.getString("concert_time")) ? "null"
							: getConcert_time(getCommon(json.getString("concert_time")));
					//
					String fav_count = isKong(json.getString("fav_count")) ? "null"
							: getCommon(json.getString("fav_count"));

					output = new Text(site_name + TAB + concert_price + TAB + performer + TAB + ship_city + TAB
							+ concert_place + TAB + concert_title + TAB + concert_time + TAB + fav_count);
					context.write(new Text(performer + TAB + concert_time), output);
				}
			}

		}
	}

	private static class Concert_Reduce extends Reducer<Text, Text, Text, Text> {

		// 将结果输出到多个文件或多个文件夹
		public MultipleOutputs<Text, Text> mos;

		// 创建MultipleOutputs对象
		public void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);
		}

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			String str = "";
			Text output;
			List<String> list = new ArrayList<String>();
			for (Text value : values) {
				sum++;
				list.add(value.toString());
			}
			if (sum == 1) {
				str = list.get(0);
			} // 若有一条，原样输出
			else {
				for (int i = 0; i < list.size(); i++)// 若有两条
				{
					if (list.get(i).split(TAB)[0].equals("damai_concert")) { // 若有多行，设定为damai的那一行数据
						str = list.get(i);
						str = str.replace("damai_concert", "yongle_damai");
					} else {
						continue;
					}
				}
			}

			output = new Text(str);
			String site_name = str.split(TAB)[0]; // 以site_name为多文件输出的依据

			if (site_name.equals("yongle_concert")) {
				mos.write("yongle", output, new Text(), "product=yongle/");
			} else if (site_name.equals("damai_concert")) {
				mos.write("damai", output, new Text(), "product=damai/");
			} else if (site_name.equals("yongle_damai")) {
				mos.write("yongledamai", output, new Text(), "product=yongledamai/");
			}

		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(Concert.class);
		job.setJobName("concert");
		job.setNumReduceTasks(10);
		job.setMapperClass(ConcertMapper.class);
		job.setReducerClass(Concert_Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleOutputs.addNamedOutput(job, "yongle", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "damai", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "yongledamai", TextOutputFormat.class, Text.class, NullWritable.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
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
		// String time = sdf.format(dBefore); // 格式化前一天

		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		// cal.add(Calendar.MONTH, -1);
		cal.set(Calendar.DAY_OF_MONTH, cal.getActualMinimum(Calendar.DAY_OF_MONTH));
		String Month_firstday = sdf.format(cal.getTime());// 例如3月运行
															// 就显示日期为20170301
		String HiveTable_out = "/music/warehouse/artist_concert/dt=" + Month_firstday;
		Path Hive_path = new Path(HiveTable_out);

		// String out = "/user/work/ArtistModel/" + time + "/Concert";
		// Path path = new Path(out);
		hdfs.delete(Hive_path, true);
		return Concert.runLoadMapReducue(conf, args[0], Hive_path);
	}

}
