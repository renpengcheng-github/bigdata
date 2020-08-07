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

public class Movie {

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
	// 处理souhu电影名
	public static String getSouhu_moviename(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("\\[\"电影：(.*)\"\\]");
			Matcher m = p.matcher(value);
			String result = "";
			while (m.find()) {
				result = m.group(1);
			}
			return result;
		} else {
			return value;
		}
	}

	// 处理souhu publish_time
	public static String getSouhu_publishtime(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("\\[\"上映时间：(.*)\"\\]");
			Matcher m = p.matcher(value);
			String result = "";
			while (m.find()) {
				result = m.group(1);
			}
			return result;
		} else {
			return value;
		}
	}

	// 处理土豆的 publish_time
	public static String getTudou_publishtime(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("\\[\"土豆上映：(.*)\"\\]");
			Matcher m = p.matcher(value);
			String result = "";
			while (m.find()) {
				result = m.group(1);
			}
			return result;
		} else {
			return value;
		}
	}

	// 优酷电影名
	public static String getYouku_film(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("\\[\"：(.*)\"\\]");
			Matcher m = p.matcher(value);
			String result = "";
			while (m.find()) {
				result = m.group(1);
			}
			return result.replace(",", "");
		} else {
			return value;
		}
	}

	// 优酷 评论
	public static String getYouku_comment(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("\\[\"评论：(.*)\"\\]");
			Matcher m = p.matcher(value);
			String result = "";
			while (m.find()) {
				result = m.group(1);
			}
			return result.replace(",", "");
		} else {
			return value;
		}
	}

	// 优酷 播放
	public static String getYouku_play(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("\\[\"总播放数：(.*)\"\\]");
			Matcher m = p.matcher(value);
			String result = "";
			while (m.find()) {
				result = m.group(1);
			}
			return result.replace(",", "");
		} else {
			return value;
		}
	}

	// 优酷 顶
	public static String getYouku_fav(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("\\[\"顶：(.*)\"\\]");
			Matcher m = p.matcher(value);
			String result = "";
			while (m.find()) {
				result = m.group(1);
			}
			return result.replace(",", "");
		} else {
			return value;
		}
	}

	// 乐视 总评论数
	public static String getLetv_comment(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("\\[\"总评论数： (.*)\"\\]");
			Matcher m = p.matcher(value);
			String result = "";
			while (m.find()) {
				result = m.group(1);
			}
			return result.replace(",", "");
		} else {
			return value;
		}
	}

	// 乐视 总播放量
	public static String getLetv_play(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("\\[\"总播放量： (.*)\"\\]");
			Matcher m = p.matcher(value);
			String result = "";
			while (m.find()) {
				result = m.group(1);
			}
			return result.replace(",", "");
		} else {
			return value;
		}
	}

	// 得到豆瓣的语言
	public static String getDouban_language(String value) {
		String[] values = getCommon(value).split(",");
		String language = null;
		for (int i = 0; i < values.length; i++) {

			if (values[i].matches(".*语.*")) {
				language = values[i].trim();
			}

		}
		return language;
	}

	// 得到豆瓣的国家
	public static String getDouban_Area(String value) {
		String[] values = getCommon(value).split(",");
		String language = null;
		for (int i = 0; i < values.length; i++) {

			if (values[i].matches(".*国.*")) {
				language = values[i].trim();
			}

		}
		return language;
	}

	// 得到豆瓣evaluation_counts
	public static String getDouban_evaluation_counts(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("\\[\"全部 (.*) 条\"\\]");
			Matcher m = p.matcher(value);
			String result = "";
			while (m.find()) {
				result = m.group(1);
			}
			return result.replace(",", "");
		} else {
			return value;
		}
	}

	public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {

		// 将结果输出到多个文件或多个文件夹
		public MultipleOutputs<Text, Text> mos;

		// 创建MultipleOutputs对象
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
				if (json.getString("site_name").matches(".*souhu_movie.*")) {
					String movie_name = isKong(json.getString("movie_name")) ? "null"
							: getSouhu_moviename(json.getString("movie_name"));
					String publish_time = isKong(json.getString("publish_time")) ? "null"
							: getSouhu_publishtime(json.getString("publish_time"));
					String primary_stars = isKong(json.getString("primary_stars")) ? "null"
							: getCommon(json.getString("primary_stars"));
					String director = isKong(json.getString("director")) ? "null"
							: getCommon(json.getString("director"));
					String area = isKong(json.getString("area")) ? "null" : getCommon(json.getString("area"));
					String style_classify = isKong(json.getString("style_classify")) ? "null"
							: getCommon(json.getString("style_classify"));
					String language = isKong(json.getString("language")) ? "null"
							: getCommon(json.getString("language"));
					String play_counts = isKong(json.getString("play_counts")) ? "null"
							: getCount(json.getString("play_counts"));
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getCount(json.getString("comment_counts"));
					String introduce = isKong(json.getString("introduce")) ? "null"
							: getCommon(json.getString("introduce"));
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: ToolUtils.FormatNumber(getCount(json.getString("fav_counts")));
					String step_counts = isKong(json.getString("step_counts")) ? "null"
							: getCount(json.getString("step_counts"));
					String score_level = isKong(json.getString("score_level")) ? "null"
							: getCount(json.getString("score_level"));
					String score_counts = isKong(json.getString("score_counts")) ? "null"
							: getCount(json.getString("score_counts"));
					String fans_counts = isKong(json.getString("fans_counts")) ? "null"
							: getCount(json.getString("fans_counts"));
					String index = isKong(json.getString("index")) ? "null" : getCommon(json.getString("index"));
					String duration = isKong(json.getString("duration")) ? "null"
							: getCommon(json.getString("duration"));
					String evaluation_counts = isKong(json.getString("evaluation_counts")) ? "null"
							: getCount(json.getString("evaluation_counts"));
					String vote_counts = isKong(json.getString("vote_counts")) ? "null"
							: getCount(json.getString("vote_counts"));
					output = new Text(site_name + TAB + movie_name + TAB + publish_time + TAB + primary_stars + TAB
							+ director + TAB + area + TAB + style_classify + TAB + language + TAB + play_counts + TAB
							+ comment_counts + TAB + introduce + TAB + fav_counts + TAB + step_counts + TAB
							+ score_level + TAB + score_counts + TAB + fans_counts + TAB + index + TAB + duration + TAB
							+ evaluation_counts + TAB + vote_counts);
					if (output.toString().split(TAB)[0].equals("souhu_movie")) {
						mos.write("souhu", output, new Text(), "product=souhu/");
					}

				} else if (json.getString("site_name").matches(".*tudou_movie.*")) {
					String movie_name = isKong(json.getString("movie_name")) ? "null"
							: getName(json.getString("movie_name"));
					String publish_time = isKong(json.getString("publish_time")) ? "null"
							: getTudou_publishtime(json.getString("publish_time"));
					String primary_stars = isKong(json.getString("primary_stars")) ? "null"
							: getCommon(json.getString("primary_stars"));
					String director = isKong(json.getString("director")) ? "null"
							: getCommon(json.getString("director"));
					String area = isKong(json.getString("area")) ? "null" : getCommon(json.getString("area"));
					String style_classify = isKong(json.getString("style_classify")) ? "null"
							: getCommon(json.getString("style_classify"));
					String language = isKong(json.getString("language")) ? "null"
							: getCommon(json.getString("language"));
					String play_counts = isKong(json.getString("play_counts")) ? "null"
							: getCount(json.getString("play_counts"));
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getCount(json.getString("comment_counts"));
					String introduce = isKong(json.getString("introduce")) ? "null"
							: getCommon(json.getString("introduce"));
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: getCount(json.getString("fav_counts"));
					String step_counts = isKong(json.getString("step_counts")) ? "null"
							: getCount(json.getString("step_counts"));
					String score_level = isKong(json.getString("score_level")) ? "null"
							: getCount(json.getString("score_level"));
					String score_counts = isKong(json.getString("score_counts")) ? "null"
							: getCount(json.getString("score_counts"));
					String fans_counts = isKong(json.getString("fans_counts")) ? "null"
							: getCount(json.getString("fans_counts"));
					String index = isKong(json.getString("index")) ? "null" : getCommon(json.getString("index"));
					String duration = isKong(json.getString("duration")) ? "null"
							: getCommon(json.getString("duration"));
					String evaluation_counts = isKong(json.getString("evaluation_counts")) ? "null"
							: getCount(json.getString("evaluation_counts"));
					String vote_counts = isKong(json.getString("vote_counts")) ? "null"
							: getCount(json.getString("vote_counts"));
					output = new Text(site_name + TAB + movie_name + TAB + publish_time + TAB + primary_stars + TAB
							+ director + TAB + area + TAB + style_classify + TAB + language + TAB + play_counts + TAB
							+ comment_counts + TAB + introduce + TAB + fav_counts + TAB + step_counts + TAB
							+ score_level + TAB + score_counts + TAB + fans_counts + TAB + index + TAB + duration + TAB
							+ evaluation_counts + TAB + vote_counts);
					if (output.toString().split(TAB)[0].equals("tudou_movie")) {
						mos.write("tudou", output, new Text(), "product=tudou/");
					}
				} else if (json.getString("site_name").matches(".*youku_movie.*")) {

					String movie_name = isKong(json.getString("movie_name")) ? "null"
							: getYouku_film(json.getString("movie_name"));
					String publish_time = isKong(json.getString("publish_time")) ? "null"
							: getCommon(json.getString("publish_time"));
					String primary_stars = isKong(json.getString("primary_stars")) ? "null"
							: getCommon(json.getString("primary_stars"));
					String director = isKong(json.getString("director")) ? "null"
							: getCommon(json.getString("director"));
					String area = isKong(json.getString("area")) ? "null" : getCommon(json.getString("area"));
					String style_classify = isKong(json.getString("style_classify")) ? "null"
							: getCommon(json.getString("style_classify"));
					String language = isKong(json.getString("language")) ? "null"
							: getCommon(json.getString("language"));
					String play_counts = isKong(json.getString("play_counts")) ? "null"
							: ToolUtils.FormatNumber(getYouku_play(json.getString("play_counts")));
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: ToolUtils.FormatNumber(getYouku_comment(json.getString("comment_counts")));
					String introduce = isKong(json.getString("introduce")) ? "null"
							: getCommon(json.getString("introduce"));
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: ToolUtils.FormatNumber(getYouku_fav(json.getString("fav_counts")));
					String step_counts = isKong(json.getString("step_counts")) ? "null"
							: getCount(json.getString("step_counts"));
					String score_level = isKong(json.getString("score_level")) ? "null"
							: getCount(json.getString("score_level"));
					String score_counts = isKong(json.getString("score_counts")) ? "null"
							: getCount(json.getString("score_counts"));
					String fans_counts = isKong(json.getString("fans_counts")) ? "null"
							: getCount(json.getString("fans_counts"));
					String index = isKong(json.getString("index")) ? "null" : getCommon(json.getString("index"));
					String duration = isKong(json.getString("duration")) ? "null"
							: getCommon(json.getString("duration"));
					String evaluation_counts = isKong(json.getString("evaluation_counts")) ? "null"
							: getCount(json.getString("evaluation_counts"));
					String vote_counts = isKong(json.getString("vote_counts")) ? "null"
							: getCount(json.getString("vote_counts"));
					output = new Text(site_name + TAB + movie_name + TAB + publish_time + TAB + primary_stars + TAB
							+ director + TAB + area + TAB + style_classify + TAB + language + TAB + play_counts + TAB
							+ comment_counts + TAB + introduce + TAB + fav_counts + TAB + step_counts + TAB
							+ score_level + TAB + score_counts + TAB + fans_counts + TAB + index + TAB + duration + TAB
							+ evaluation_counts + TAB + vote_counts);
					if (output.toString().split(TAB)[0].equals("youku_movie")) {
						mos.write("youku", output, new Text(), "product=youku/");
					}

				} else if (json.getString("site_name").matches(".*letv_movie.*")) {

					String movie_name = isKong(json.getString("movie_name")) ? "null"
							: getCommon(json.getString("movie_name"));
					String publish_time = isKong(json.getString("publish_time")) ? "null"
							: getCommon(json.getString("publish_time"));
					String primary_stars = isKong(json.getString("primary_stars")) ? "null"
							: getCommon(json.getString("primary_stars"));
					String director = isKong(json.getString("director")) ? "null"
							: getCommon(json.getString("director"));
					String area = isKong(json.getString("area")) ? "null" : getCommon(json.getString("area"));
					String style_classify = isKong(json.getString("style_classify")) ? "null"
							: getCommon(json.getString("style_classify"));
					String language = isKong(json.getString("language")) ? "null"
							: getCommon(json.getString("language"));
					String play_counts = isKong(json.getString("play_counts")) ? "null"
							: ToolUtils.FormatNumber(getLetv_play(json.getString("play_counts")));
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: ToolUtils.FormatNumber(getLetv_comment(json.getString("comment_counts")));
					String introduce = isKong(json.getString("introduce")) ? "null"
							: getCommon(json.getString("introduce"));
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: getCount(json.getString("fav_counts"));
					String step_counts = isKong(json.getString("step_counts")) ? "null"
							: getCount(json.getString("step_counts"));
					String score_level = isKong(json.getString("score_level")) ? "null"
							: getCount(json.getString("score_level"));
					String score_counts = isKong(json.getString("score_counts")) ? "null"
							: getCount(json.getString("score_counts"));
					String fans_counts = isKong(json.getString("fans_counts")) ? "null"
							: getCount(json.getString("fans_counts"));
					String index = isKong(json.getString("index")) ? "null" : getCommon(json.getString("index"));
					String duration = isKong(json.getString("duration")) ? "null"
							: getCommon(json.getString("duration"));
					String evaluation_counts = isKong(json.getString("evaluation_counts")) ? "null"
							: getCount(json.getString("evaluation_counts"));
					String vote_counts = isKong(json.getString("vote_counts")) ? "null"
							: getCount(json.getString("vote_counts"));
					output = new Text(site_name + TAB + movie_name + TAB + publish_time + TAB + primary_stars + TAB
							+ director + TAB + area + TAB + style_classify + TAB + language + TAB + play_counts + TAB
							+ comment_counts + TAB + introduce + TAB + fav_counts + TAB + step_counts + TAB
							+ score_level + TAB + score_counts + TAB + fans_counts + TAB + index + TAB + duration + TAB
							+ evaluation_counts + TAB + vote_counts);
					if (output.toString().split(TAB)[0].equals("letv_movie")) {
						mos.write("letv", output, new Text(), "product=letv/");
					}

				} else if (json.getString("site_name").matches(".*mangguo_movie.*")) {

					String movie_name = isKong(json.getString("movie_name")) ? "null"
							: getCommon(json.getString("movie_name"));
					String publish_time = isKong(json.getString("publish_time")) ? "null"
							: getCommon(json.getString("publish_time"));
					String primary_stars = isKong(json.getString("primary_stars")) ? "null"
							: getCommon(json.getString("primary_stars"));
					String director = isKong(json.getString("director")) ? "null"
							: getCommon(json.getString("director"));
					String area = isKong(json.getString("area")) ? "null" : getCommon(json.getString("area"));
					String style_classify = isKong(json.getString("style_classify")) ? "null"
							: getCommon(json.getString("style_classify"));
					String language = isKong(json.getString("language")) ? "null"
							: getCommon(json.getString("language"));
					String play_counts = isKong(json.getString("play_counts")) ? "null"
							: getCount(json.getString("play_counts"));
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getCount(json.getString("comment_counts"));
					String introduce = isKong(json.getString("introduce")) ? "null"
							: getCommon(json.getString("introduce"));
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: getCount(json.getString("fav_counts"));
					String step_counts = isKong(json.getString("step_counts")) ? "null"
							: getCount(json.getString("step_counts"));
					String score_level = isKong(json.getString("score_level")) ? "null"
							: getCount(json.getString("score_level"));
					String score_counts = isKong(json.getString("score_counts")) ? "null"
							: getCount(json.getString("score_counts"));
					String fans_counts = isKong(json.getString("fans_counts")) ? "null"
							: getCount(json.getString("fans_counts"));
					String index = isKong(json.getString("index")) ? "null" : getCommon(json.getString("index"));
					String duration = isKong(json.getString("duration")) ? "null"
							: getCommon(json.getString("duration"));
					String evaluation_counts = isKong(json.getString("evaluation_counts")) ? "null"
							: getCount(json.getString("evaluation_counts"));
					String vote_counts = isKong(json.getString("vote_counts")) ? "null"
							: getCount(json.getString("vote_counts"));
					output = new Text(site_name + TAB + movie_name + TAB + publish_time + TAB + primary_stars + TAB
							+ director + TAB + area + TAB + style_classify + TAB + language + TAB + play_counts + TAB
							+ comment_counts + TAB + introduce + TAB + fav_counts + TAB + step_counts + TAB
							+ score_level + TAB + score_counts + TAB + fans_counts + TAB + index + TAB + duration + TAB
							+ evaluation_counts + TAB + vote_counts);
					if (output.toString().split(TAB)[0].equals("mangguo_movie")) {
						mos.write("mangguo", output, new Text(), "product=mangguo/");
					}
				} else if (json.getString("site_name").matches(".*douban_movie.*")) {

					String movie_name = isKong(json.getString("movie_name")) ? "null"
							: getCommon(json.getString("movie_name"));
					String publish_time = isKong(json.getString("publish_time")) ? "null"
							: getCommon(json.getString("publish_time"));
					String primary_stars = isKong(json.getString("primary_stars")) ? "null"
							: getCommon(json.getString("primary_stars"));
					String director = isKong(json.getString("director")) ? "null"
							: getCommon(json.getString("director"));
					String area = isKong(json.getString("area")) ? "null" : getDouban_Area(json.getString("area"));
					String style_classify = isKong(json.getString("style_classify")) ? "null"
							: getCommon(json.getString("style_classify"));
					String language = isKong(json.getString("language")) ? "null"
							: getDouban_language(json.getString("language"));
					String play_counts = isKong(json.getString("play_counts")) ? "null"
							: getCount(json.getString("play_counts"));
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getCount(json.getString("comment_counts"));
					String introduce = isKong(json.getString("introduce")) ? "null"
							: getCommon(json.getString("introduce"));
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: ToolUtils.FormatNumber(getCount(json.getString("fav_counts")));
					String step_counts = isKong(json.getString("step_counts")) ? "null"
							: getCount(json.getString("step_counts"));
					String score_level = isKong(json.getString("score_level")) ? "null"
							: getCount(json.getString("score_level"));
					String score_counts = isKong(json.getString("score_counts")) ? "null"
							: getCount(json.getString("score_counts"));
					String fans_counts = isKong(json.getString("fans_counts")) ? "null"
							: getCount(json.getString("fans_counts"));
					String index = isKong(json.getString("index")) ? "null" : getCommon(json.getString("index"));
					String duration = isKong(json.getString("duration")) ? "null"
							: getCommon(json.getString("duration"));
					String evaluation_counts = isKong(json.getString("evaluation_counts")) ? "null"
							: getDouban_evaluation_counts(json.getString("evaluation_counts"));
					String vote_counts = isKong(json.getString("vote_counts")) ? "null"
							: getCount(json.getString("vote_counts"));
					output = new Text(site_name + TAB + movie_name + TAB + publish_time + TAB + primary_stars + TAB
							+ director + TAB + area + TAB + style_classify + TAB + language + TAB + play_counts + TAB
							+ comment_counts + TAB + introduce + TAB + fav_counts + TAB + step_counts + TAB
							+ score_level + TAB + score_counts + TAB + fans_counts + TAB + index + TAB + duration + TAB
							+ evaluation_counts + TAB + vote_counts);
					if (output.toString().split(TAB)[0].equals("douban_movie")) {
						mos.write("douban", output, new Text(), "product=douban/");
					}

				} else if (json.getString("site_name").matches(".*aiqiyi_movie.*")
						|| json.getString("site_name").matches(".*tencent_movie.*")) {

					String movie_name = isKong(json.getString("movie_name")) ? "null"
							: getCommon(json.getString("movie_name"));
					String publish_time = isKong(json.getString("publish_time")) ? "null"
							: getCommon(json.getString("publish_time"));
					String primary_stars = isKong(json.getString("primary_stars")) ? "null"
							: getName(json.getString("primary_stars"));
					String director = isKong(json.getString("director")) ? "null" : getName(json.getString("director"));
					String area = isKong(json.getString("area")) ? "null" : getCommon(json.getString("area"));
					String style_classify = isKong(json.getString("style_classify")) ? "null"
							: getCommon(json.getString("style_classify"));
					String language = isKong(json.getString("language")) ? "null"
							: getCommon(json.getString("language"));
					String play_counts = isKong(json.getString("play_counts")) ? "null"
							: getCount(json.getString("play_counts"));
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getCount(json.getString("comment_counts"));
					String introduce = isKong(json.getString("introduce")) ? "null"
							: getCommon(json.getString("introduce"));
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: getCount(json.getString("fav_counts"));
					String step_counts = isKong(json.getString("step_count")) ? "null"
							: getCount(json.getString("step_count"));
					String score_level = isKong(json.getString("score_level")) ? "null"
							: getCount(json.getString("score_level"));
					String score_counts = isKong(json.getString("score_counts")) ? "null"
							: getCount(json.getString("score_counts"));
					String fans_counts = isKong(json.getString("fans_counts")) ? "null"
							: getCount(json.getString("fans_counts"));
					String index = isKong(json.getString("index")) ? "null" : getCommon(json.getString("index"));
					String duration = isKong(json.getString("duration")) ? "null"
							: getCommon(json.getString("duration"));
					String evalution_counts = isKong(json.getString("evalution_counts")) ? "null"
							: getCount(json.getString("evalution_counts"));
					String vote_counts = isKong(json.getString("vote_counts")) ? "null"
							: getCount(json.getString("vote_counts"));
					output = new Text(site_name + TAB + movie_name + TAB + publish_time + TAB + primary_stars + TAB
							+ director + TAB + area + TAB + style_classify + TAB + language + TAB + play_counts + TAB
							+ comment_counts + TAB + introduce + TAB + fav_counts + TAB + step_counts + TAB
							+ score_level + TAB + score_counts + TAB + fans_counts + TAB + index + TAB + duration + TAB
							+ evalution_counts + TAB + vote_counts);
					if (output.toString().split(TAB)[0].equals("aiqiyi_movie")) {
						mos.write("aiqiyi", output, new Text(), "product=aiqiyi/");
					} else if (output.toString().split(TAB)[0].equals("tencent_movie")) {
						mos.write("qq", output, new Text(), "product=qq/");
					}
				}
			}

		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(Movie.class);
		job.setJobName("Movie");
		job.setNumReduceTasks(1);
		job.setMapperClass(MovieMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleOutputs.addNamedOutput(job, "douban", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "mangguo", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "letv", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "youku", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "souhu", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "tudou", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "aiqiyi", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "qq", TextOutputFormat.class, Text.class, NullWritable.class);
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
		String HiveTable_out = "/music/warehouse/music_artistmodel_movie/dt=" + Month_firstday;
		Path Hive_path = new Path(HiveTable_out);
		// String time = sdf.format(dBefore); // 格式化前一天
		// String out = "/user/work/ArtistModel/" + time + "/Movie";
		// Path path = new Path(out);
		hdfs.delete(Hive_path, true);
		return Movie.runLoadMapReducue(conf, args[0], Hive_path);
	}

}
