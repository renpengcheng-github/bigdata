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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.alibaba.fastjson.JSONObject;
import Spider.ToolUtils;

public class Media_show {

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
	public static String get_spile_nums(String value) { // 取冒号后面的东西
														// 数字自动处理逗号，万，亿等
		if (value != null) {
			Pattern p = Pattern.compile("(.*)(:|：)(.*)");
			Matcher m = p.matcher(value);
			String result = "";
			while (m.find()) {
				result = m.group(3);
			}
			return getCount(result);
		} else {
			return null;
		}
	}

	public static String getAiqiyi_update_time(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("(^[^\u4e00-\u9fa5]*)");
			Matcher m = p.matcher(value);
			String result = "";
			while (m.find()) {
				result = m.group(1);
			}
			return getCount(result);
		} else {
			return value;
		}
	}
	public static String getAiqiyi_showname(String value) {
		String result[] = null;
		if (value != null) {
			String reg = "(.*?),(.*?)";
			if (value.matches(reg)) {
				result=value.split(",");
			}
			else{
				return value;
			}

		}
		return result[0];
	}


	public static final String TAB = "\001";

	public static class MediaMapper extends Mapper<LongWritable, Text, Text, Text> {

		// 将结果输出到多个文件或多个文件夹
		public MultipleOutputs<Text, Text> mos;

		// 创建 MultipleOutputs 对象
		public void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);
		}

		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Text output;
			if (value.toString().matches("\\{.*\\}")) {
				JSONObject json = JSONObject.parseObject(value.toString());
				// 网站标示
				String site_name = isKong(json.getString("site_name")) ? "null"
						: getCommon(json.getString("site_name"));
				// 芒果 腾讯 土豆
				if (site_name.matches(".*mangguo_show.*") || site_name.matches(".*tencent_show.*")
						|| site_name.matches(".*tudou_show.*")) {
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getCount(json.getString("comment_counts"));
					String score_counts = isKong(json.getString("score_counts")) ? "null"
							: getCount(json.getString("score_counts"));
					String update_time = isKong(json.getString("update_time")) ? "null"
							: getCommon(json.getString("update_time"));
					String play_counts = isKong(json.getString("play_counts")) ? "null"
							: getCount(json.getString("play_counts"));
					String fans_counts = isKong(json.getString("fans_counts")) ? "null"
							: getCount(json.getString("fans_counts"));
					String show_introduce = isKong(json.getString("show_introduce")) ? "null"
							: getCommon(json.getString("show_introduce"));
					String show_name = isKong(json.getString("show_name")) ? "null"
							: getName(json.getString("show_name"));
					String style_classify = isKong(json.getString("style_classify")) ? "null"
							: getCommon(json.getString("style_classify"));
					String host = isKong(json.getString("host")) ? "null" : getName(json.getString("host"));
					String area = isKong(json.getString("area")) ? "null" : getName(json.getString("area"));
					String score_level = isKong(json.getString("score_level")) ? "null"
							: getCount(json.getString("score_level"));
					String first_show = isKong(json.getString("first_show")) ? "null"
							: getCommon(json.getString("first_show"));
					String tv_station = isKong(json.getString("tv_station")) ? "null"
							: getCommon(json.getString("tv_station"));
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: getCount(json.getString("fav_counts"));
					output = new Text(site_name + TAB + comment_counts + TAB + score_counts + TAB + update_time + TAB
							+ play_counts + TAB + fans_counts + TAB + show_introduce + TAB + show_name + TAB
							+ style_classify + TAB + host + TAB + area + TAB + score_level + TAB + first_show + TAB
							+ tv_station + TAB + fav_counts);

					if (output.toString().split(TAB)[0].equals("mangguo_show")) {
						mos.write("mangguo", output, new Text(), "product=mangguo/");
					} else if (output.toString().split(TAB)[0].equals("tudou_show")) {
						mos.write("tudou", output, new Text(), "product=tudou/");
					} else if (output.toString().split(TAB)[0].equals("tencent_show")) {
						mos.write("tencent", output, new Text(), "product=qq/");
					}

				}
				// 搜狐
				else if (site_name.matches(".*souhu_show.*")) {
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getCount(json.getString("comment_counts"));
					String score_counts = isKong(json.getString("score_counts")) ? "null"
							: getCount(json.getString("score_counts"));
					String update_time = isKong(json.getString("update_time")) ? "null"
							: getCommon(json.getString("update_time"));
					String play_counts = isKong(json.getString("play_counts")) ? "null"
							: getCount(json.getString("play_counts"));
					String fans_counts = isKong(json.getString("fans_counts")) ? "null"
							: getCount(json.getString("fans_counts"));
					String show_introduce = isKong(json.getString("show_introduce")) ? "null"
							: getCommon(json.getString("show_introduce"));
					String show_name = isKong(json.getString("show_name")) ? "null"
							: get_spile_nums(getName(json.getString("show_name")));
					String style_classify = isKong(json.getString("style_classify")) ? "null"
							: getCommon(json.getString("style_classify"));
					String host = isKong(json.getString("host")) ? "null" : getName(json.getString("host"));
					String area = isKong(json.getString("area")) ? "null" : getName(json.getString("area"));
					String score_level = isKong(json.getString("score_level")) ? "null"
							: getCommon(json.getString("score_level"));
					String first_show = isKong(json.getString("first_show")) ? "null"
							: getCommon(json.getString("first_show"));
					String tv_station = isKong(json.getString("tv_station")) ? "null"
							: getCommon(json.getString("tv_station"));
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: getCount(json.getString("fav_counts"));
					output = new Text(site_name + TAB + comment_counts + TAB + score_counts + TAB + update_time + TAB
							+ play_counts + TAB + fans_counts + TAB + show_introduce + TAB + show_name + TAB
							+ style_classify + TAB + host + TAB + area + TAB + score_level + TAB + first_show + TAB
							+ tv_station + TAB + fav_counts);

					if (output.toString().split(TAB)[0].equals("souhu_show")) {
						mos.write("souhu", output, new Text(), "product=souhu/");
					}

				}
				// 乐视
				else if (site_name.matches(".*letv_show.*")) {
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: get_spile_nums(getCount(json.getString("comment_counts")));
					String score_counts = isKong(json.getString("score_counts")) ? "null"
							: getCount(json.getString("score_counts"));
					String update_time = isKong(json.getString("update_time")) ? "null"
							: getCommon(json.getString("update_time"));
					String play_counts = isKong(json.getString("play_counts")) ? "null"
							: get_spile_nums(getCount(json.getString("play_counts")));
					String fans_counts = isKong(json.getString("fans_counts")) ? "null"
							: getCount(json.getString("fans_counts"));
					String show_introduce = isKong(json.getString("show_introduce")) ? "null"
							: getCommon(json.getString("show_introduce"));
					String show_name = isKong(json.getString("show_name")) ? "null"
							: getName(json.getString("show_name"));
					String style_classify = isKong(json.getString("style_classify")) ? "null"
							: getCommon(json.getString("style_classify"));
					String host = isKong(json.getString("host")) ? "null" : getName(json.getString("host"));
					String area = isKong(json.getString("area")) ? "null" : getName(json.getString("area"));
					String score_level = isKong(json.getString("score_level")) ? "null"
							: getCount(json.getString("score_level"));
					String first_show = isKong(json.getString("first_show")) ? "null"
							: getCommon(json.getString("first_show"));
					String tv_station = isKong(json.getString("tv_station")) ? "null"
							: getCommon(json.getString("tv_station"));
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: getCount(json.getString("fav_counts"));
					output = new Text(site_name + TAB + comment_counts + TAB + score_counts + TAB + update_time + TAB
							+ play_counts + TAB + fans_counts + TAB + show_introduce + TAB + show_name + TAB
							+ style_classify + TAB + host + TAB + area + TAB + score_level + TAB + first_show + TAB
							+ tv_station + TAB + fav_counts);
					if (output.toString().split(TAB)[0].equals("letv_show")) {
						mos.write("letv", output, new Text(), "product=letv/");
					}

					// 优酷
				} else if (site_name.matches(".*youku_show.*")) {
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: get_spile_nums(getCount(json.getString("comment_counts")));
					String score_counts = isKong(json.getString("score_counts")) ? "null"
							: getCount(json.getString("score_counts"));
					String update_time = isKong(json.getString("update_time")) ? "null"
							: getCommon(json.getString("update_time"));
					String play_counts = isKong(json.getString("play_counts")) ? "null"
							: get_spile_nums(getCount(json.getString("play_counts")));
					String fans_counts = isKong(json.getString("fans_counts")) ? "null"
							: getCount(json.getString("fans_counts"));
					String show_introduce = isKong(json.getString("show_introduce")) ? "null"
							: getCommon(json.getString("show_introduce"));
					String show_name = isKong(json.getString("show_name")) ? "null"
							: getName(json.getString("show_name")).replace(":", "").replace("：", "");
					String style_classify = isKong(json.getString("style_classify")) ? "null"
							: getCommon(json.getString("style_classify"));
					String host = isKong(json.getString("host")) ? "null" : getName(json.getString("host"));
					String area = isKong(json.getString("area")) ? "null" : getName(json.getString("area"));
					String score_level = isKong(json.getString("score_level")) ? "null"
							: getCommon(json.getString("score_level"));
					String first_show = isKong(json.getString("first_show")) ? "null"
							: getCommon(json.getString("first_show"));
					String tv_station = isKong(json.getString("tv_station")) ? "null"
							: getCommon(json.getString("tv_station"));
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: get_spile_nums(getCount(json.getString("fav_counts")));
					output = new Text(site_name + TAB + comment_counts + TAB + score_counts + TAB + update_time + TAB
							+ play_counts + TAB + fans_counts + TAB + show_introduce + TAB + show_name + TAB
							+ style_classify + TAB + host + TAB + area + TAB + score_level + TAB + first_show + TAB
							+ tv_station + TAB + fav_counts);
					if (output.toString().split(TAB)[0].equals("youku_show")) {
						mos.write("youku", output, new Text(), "product=youku/");
					}

				}

				else if (site_name.matches(".*aiqiyi_show.*")) {
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getCount(json.getString("comment_counts"));
					String score_counts = isKong(json.getString("score_counts")) ? "null"
							: getCount(json.getString("score_counts"));
					String update_time = isKong(json.getString("update_time")) ? "null"
							: getAiqiyi_update_time(getCommon(json.getString("update_time")));
					String play_counts = isKong(json.getString("play_counts")) ? "null"
							: getCount(json.getString("play_counts"));
					String fans_counts = isKong(json.getString("fans_counts")) ? "null"
							: getCount(json.getString("fans_counts"));
					String show_introduce = isKong(json.getString("show_introduce")) ? "null"
							: getCommon(json.getString("show_introduce"));
					String show_name = isKong(json.getString("show_name")) ? "null"
							: getAiqiyi_showname(getName(json.getString("show_name")));
					String style_classify = isKong(json.getString("style_classify")) ? "null"
							: getCommon(json.getString("style_classify"));
					String host = isKong(json.getString("host")) ? "null" : getName(json.getString("host"));
					String area = isKong(json.getString("area")) ? "null" : getName(json.getString("area"));
					//处理抓日期时将日期抓到area字段的情况
					String reg="(,*)期";
					if((update_time.equals("null")||update_time.trim().isEmpty())&&area.matches(reg)){
						update_time=getAiqiyi_update_time(getCommon(json.getString("area")));
					}
					String score_level = isKong(json.getString("score_level")) ? "null"
							: getCount(json.getString("score_level"));
					String first_show = isKong(json.getString("first_show")) ? "null"
							: getCommon(json.getString("first_show"));
					String tv_station = isKong(json.getString("tv_station")) ? "null"
							: getCommon(json.getString("tv_station"));
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: getCount(json.getString("fav_counts"));
					output = new Text(site_name + TAB + comment_counts + TAB + score_counts + TAB + update_time + TAB
							+ play_counts + TAB + fans_counts + TAB + show_introduce + TAB + show_name + TAB
							+ style_classify + TAB + host + TAB + area + TAB + score_level + TAB + first_show + TAB
							+ tv_station + TAB + fav_counts);
					if (output.toString().split(TAB)[0].equals("aiqiyi_show")) {
						mos.write("aiqiyi", output, new Text(), "product=aiqiyi/");
					}

				}
			}
		}

	}

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(Media_show.class);
		job.setJobName("Media");
		job.setNumReduceTasks(1);
		job.setMapperClass(MediaMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleOutputs.addNamedOutput(job, "youku", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "letv", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "tencent", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "tudou", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "mangguo", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "souhu", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "aiqiyi", TextOutputFormat.class, Text.class, NullWritable.class);
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

		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		// cal.add(Calendar.MONTH, -1);
		cal.set(Calendar.DAY_OF_MONTH, cal.getActualMinimum(Calendar.DAY_OF_MONTH));
		String Month_firstday = sdf.format(cal.getTime());// 例如3月运行就显示日期为20170301
		String HiveTable_out = "/music/warehouse/music_artistmodel_media/dt=" + Month_firstday;
		Path Hive_path = new Path(HiveTable_out);
		// String time = sdf.format(dBefore); // 格式化前一天
		// String out = "/user/work/ArtistModel/" + time + "/Media";
		// Path path = new Path(out);
		hdfs.delete(Hive_path, true);
		return Media_show.runLoadMapReducue(conf, args[0], Hive_path);
	}

}
