package Spider;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import Spider.ToolUtils;
import com.alibaba.fastjson.JSONObject;

public class Artist {

	public static final String TAB = "\001";

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


	// 虾米music 艺人名
	public static boolean isKong_xiamimusic(String value) {
		if (value == null || value.matches("\\[\"\"\\]") || value.trim().matches("\\[\"\",\"\"\\]")) {
			return true;
		} else {
			return false;
		}

	}

	public static class MusicMapper extends Mapper<LongWritable, Text, Text, Text> {
		private HashMap<String, List<String>> map;

		// 将结果输出到多个文件或多个文件夹
		public MultipleOutputs<Text, Text> mos;

		// 创建MultipleOutputs对象
		public void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);
			super.setup(context);
			File file = new File("artist_isdisplay");
			BufferedReader br = new BufferedReader(new FileReader(file));
			map = new HashMap<String, List<String>>();
			String line = "";
			// 有效艺人表分隔符为\t
			String TAB2 = "\t";
			while ((line = br.readLine()) != null) {
				String[] part = line.split(TAB2);
				List<String> partlist = new ArrayList<String>();
				for (int i = 0; i < part.length; i++) {
					partlist.add(i, part[i]);
				}
				// 将part改为可变长度的arraylist 有可能part的长度为3，没有part[3]就会报错
				// 在这里加一层判断和赋值
				if (partlist.size() < 4) {
					partlist.add(3, "null");
				}
				String artist_name[] = partlist.get(2).split(",");
				// hashmap的value设置为一个list，存储主要艺人名和艺人说明列
				List<String> all = new ArrayList<String>();
				all.add(0, artist_name[0]);
				all.add(1, partlist.get(3));
				// 循环将所有艺人名加入hashmap
				// A,A1,A2 A为主要艺人名
				// 输出map，{A,A},{A1,A}，{A2,A}
				// 注意加入{A,A},若无{A,A}则含有A的原名就不能匹配到
				// 且用来匹配的键转化为小写，以小写匹配
				for (int i = 0; i < artist_name.length; i++) {
					if (artist_name[i] != null && artist_name[i] != "null"
							&& artist_name[i].trim().isEmpty() == false) {
						map.put(artist_name[i].toLowerCase(), all);
					}
				}
			}
			br.close();
		}

		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Text output;

			if (value.toString().matches("\\{.*\\}")) {
				JSONObject json = JSONObject.parseObject(value.toString());
				// 网站标示
				String site_name = isKong(json.getString("site_name")) ? "null"
						: getCommon(json.getString("site_name"));
				if (site_name.matches(".*douban.*") || site_name.matches(".*xiami_music.*")
						|| site_name.matches(".*wangyiyun_music.*")) {
					// 艺人名
					String artist_name = isKong(json.getString("artist_name")) ? "null"
							: getName(json.getString("artist_name"));
					// 更新时间
					String update_time = isKong(json.getString("update_time")) ? "null"
							: getCommon(json.getString("update_time"));
					// 粉丝量、喜欢量
					String fans_counts = isKong(json.getString("fans_counts")) ? "null"
							: getCount(json.getString("fans_counts"));
					// 总专辑量
					String album_counts = isKong(json.getString("album_counts")) ? "null"
							: getCount(json.getString("album_counts"));
					// 总单曲量
					String song_counts = isKong(json.getString("song_counts")) ? "null"
							: getCount(json.getString("song_counts"));
					// MV量
					String mv_counts = isKong(json.getString("mv_counts")) ? "null"
							: getCount(json.getString("mv_counts"));
					// 动态量
					String dynamic_counts = isKong(json.getString("dynamic_counts")) ? "null"
							: getCount(json.getString("dynamic_counts"));
					// 评论量
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getCount(json.getString("comment_counts"));
					// 艺人总播放量
					String play_counts = isKong(json.getString("play_counts")) ? "null"
							: getCount(json.getString("play_counts"));
					// 流派
					String style_classify = isKong(json.getString("style_classify")) ? "null"
							: getCommon(json.getString("style_classify"));
					// 歌曲名
					String song_name = isKong(json.getString("song_name")) ? "null"
							: getName(json.getString("song_name")).replace(",", "");
					// Hashmap匹配，匹配不上不输出，匹配上输出value对应的值
					String isdisplay = "null";
					String artist_note = "null";
					String artist_name_lower = artist_name.toLowerCase();
					if (map != null || map.isEmpty() == false) {
						if (map.get(artist_name_lower) != null) {
							artist_name = map.get(artist_name_lower).get(0);
							isdisplay = "1";
							artist_note = map.get(artist_name_lower).get(1);
						} else {
							isdisplay = "0";
						}
					}
					output = new Text(site_name + TAB + artist_name + TAB + update_time + TAB + fans_counts + TAB
							+ album_counts + TAB + song_counts + TAB + mv_counts + TAB + dynamic_counts + TAB
							+ comment_counts + TAB + play_counts + TAB + style_classify + TAB + song_name + TAB
							+ isdisplay + TAB + artist_note);
					if (output.toString().split(TAB)[0].equals("douban_artist")) {
						mos.write("doubanartist", output, new Text(), "product=douban/");
					} else if (output.toString().split(TAB)[0].equals("xiami_artist")) {
						mos.write("xiamiartist", output, new Text(), "product=xiami/");
					} else if (output.toString().split(TAB)[0].equals("wangyiyun_artist")) {
						mos.write("wangyiartist", output, new Text(), "product=wangyi/");
					}

				}
			}
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(Artist.class);
		job.setJobName("Artist");
		job.setNumReduceTasks(1);
		job.setMapperClass(MusicMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		String artist_isdisplay_table = "/music/warehouse/music_artistmodel_artist_isdiplay/event_day=20170301";
		job.addCacheFile(new URI(artist_isdisplay_table + "/part-m-00000#artist_isdisplay"));
		MultipleOutputs.addNamedOutput(job, "doubanartist", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "xiamiartist", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "wangyiartist", TextOutputFormat.class, Text.class, NullWritable.class);
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
		String Month_firstday = sdf.format(cal.getTime());// 例如3月运行
															// 就显示日期为20170301
		//
		String HiveTable_out = "/music/warehouse/music_artistmodel_music1/dt=" + Month_firstday;
		Path Hive_path = new Path(HiveTable_out);
		// String time = sdf.format(dBefore); // 格式化前一天
		// String out = "/user/work/ArtistModel/" + time + "/Artist";
		// Path path = new Path(out);
		hdfs.delete(Hive_path, true);
		return Artist.runLoadMapReducue(conf, args[0], Hive_path);
	}

}
