package Spider;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import Spider.ToolUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;


//进行报表层的艺人匹配，暂时未使用
public class Artist_Isdisplay {

	public static final String TAB = "\t";


	// -------------------------------------------------------------------------------------------------------------------------------------------
	public static class Artist_displayMapper extends Mapper<Object, BytesRefArrayWritable, Text, Text> {
		private HashMap<String, String> map;

		private StringBuilder MyStringBuilder = new StringBuilder();

		// 将结果输出到多个文件或多个文件夹
		public MultipleOutputs<Text, Text> mos;

		protected void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);
			super.setup(context);
			File file = new File("min.max");
			BufferedReader br = new BufferedReader(new FileReader(file));
			map = new HashMap<String, String>();
			String line = "";
			// A,A1,A2 A为主要艺人名
			// 输出map，{A1,A}，{A2,A}
			while ((line = br.readLine()) != null) {
				String[] part = line.split(TAB);
				String artist_name[] = part[1].split(",");
				for (int i = 1; i < artist_name.length; i++) {
					if (artist_name[i] != null && artist_name[i] != "null") {
						map.put(artist_name[i], artist_name[0]);
					}
				}
			}
			br.close();
		}

		public void map(Object key, BytesRefArrayWritable value, Context context)
				throws IOException, InterruptedException {
			Text txt = new Text();
			String str = null;
			Text output;
			// 得到艺人名
			BytesRefWritable artist_name1 = value.get(0);
			txt.set(artist_name1.getData(), artist_name1.getStart(), artist_name1.getLength());
			String artist_name = txt.toString();
			// 得到表中艺人名以外所有数据,从site_name开始
			BytesRefWritable site_name1 = value.get(0);
			txt.set(site_name1.getData(), site_name1.getStart(), site_name1.getLength());
			String site_name = txt.toString();
			MyStringBuilder.append(site_name);
			// 0不用取，1单独处理，下标从2开始
			for (int i = 2; i < 11; i++) {
				BytesRefWritable index = value.get(i);
				txt.set(index.getData(), index.getStart(), index.getLength());
				MyStringBuilder.append(TAB + txt.toString());
			}
			// Hashmap匹配，匹配不上不输出，匹配上输出value对应的值
			if (map != null || map.isEmpty() == false) {
				if (map.get(artist_name) == null) {
					str = artist_name + MyStringBuilder.toString();
				} else {
					str = map.get(artist_name) + MyStringBuilder.toString();
				}
			}
			output = new Text(str);
			if (site_name.equals("qq_music")) {
				mos.write("qq", output, new Text(), "product=qq/");
			} else if (site_name.equals("wanyiyun_music")) {
				mos.write("wangyiyun", output, new Text(), "product=wangyi/");
			} else if (site_name.equals("douban")) {
				mos.write("douban", output, new Text(), "product=douban/");
			} else if (site_name.equals("xiami_music")) {
				mos.write("xiami", output, new Text(), "product=xiami/");
			}
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(Concert.class);
		job.setJobName("artist_isdisplay");
		job.setNumReduceTasks(1);
		job.setMapperClass(Artist_displayMapper.class);
		// job.setReducerClass(Concert_Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleOutputs.addNamedOutput(job, "qq", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "wangyiyun", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "douban", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "xiami", TextOutputFormat.class, Text.class, NullWritable.class);
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

		hdfs.delete(Hive_path, true);
		return Artist_Isdisplay.runLoadMapReducue(conf, args[0], Hive_path);
	}

}
