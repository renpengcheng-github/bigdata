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
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;
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

public class MV {

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
	// 处理爱奇艺时间
	public static String getAiqiyiTime(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("\\[\"发布时间：(.*)\"\\]");
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

	// 处理music_station时间
	public static String getMusic_stationTime(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("\\[\"发布于(.*)\"\\]");
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

	// 处理b站 粉丝
	public static String getBilibiliFans(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("\\[\"粉丝：(.*)\"\\]");
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

	// 处理b站 投稿
	public static String getBilibiliTougao(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("\\[\"投稿：(.*)\"\\]");
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

	// 处理优酷评论数
	public static String getYouku_comment(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("\\[\"(.*)次评论\"\\]");
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

	public static class MvMapper extends Mapper<LongWritable, Text, Text, Text> {

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
				// 处理A站 乐视 qq_mv xiami_mv
				if (json.getString("site_name").matches(".*letv.*") || json.getString("site_name").matches(".*qq_mv.*")
						|| json.getString("site_name").matches(".*xiami_mv.*")) {
					// 艺人名
					String artist_name = isKong(json.getString("artist_name")) ? "null"
							: getName(json.getString("artist_name"));
					// 更新时间
					String update_time = isKong(json.getString("update_time")) ? "null"
							: getCommon(json.getString("update_time"));
					// mv名称
					String mv_name = isKong(json.getString("mv_name")) ? "null" : getCommon(json.getString("mv_name"));
					// 播放量
					String play_counts = isKong(json.getString("play_counts")) ? "null"
							: getCount(json.getString("play_counts"));
					// 收藏量
					String collect_counts = isKong(json.getString("collect_counts")) ? "null"
							: getCount(json.getString("collect_counts"));
					// 点赞量
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: getCount(json.getString("fav_counts"));
					// 下载量
					String download_counts = isKong(json.getString("download_counts")) ? "null"
							: getCount(json.getString("download_counts"));
					// 发布时间
					String publish_time = isKong(json.getString("publish_time")) ? "null"
							: getCommon(json.getString("publish_time"));
					// 评论量
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getCount(json.getString("comment_counts"));
					// MV_ID
					String mv_id = isKong(json.getString("mv_id")) ? "null" : getCommon(json.getString("mv_id"));
					// 分类
					String style_classify = isKong(json.getString("style_classify")) ? "null"
							: getCommon(json.getString("style_classify"));
					// 投蕉量、硬币量
					String reward_counts = isKong(json.getString("reward_counts")) ? "null"
							: getCount(json.getString("reward_counts"));
					// 简介
					String introduce = isKong(json.getString("introduce")) ? "null"
							: getCommon(json.getString("introduce"));
					// 发布者
					String publish_name = isKong(json.getString("publish_name")) ? "null"
							: getCommon(json.getString("publish_name"));
					// 发布者id
					String publish_id = isKong(json.getString("publish_id")) ? "null"
							: getCommon(json.getString("publish_id"));
					// 投稿数
					String sub_counts = isKong(json.getString("sub_counts")) ? "null"
							: getCount(json.getString("sub_counts"));
					// 发布粉丝数
					String fans_counts = isKong(json.getString("fans_counts")) ? "null"
							: getCount(json.getString("fans_counts"));
					// 充电人数
					String charge_counts = isKong(json.getString("charge_counts")) ? "null"
							: getCount(json.getString("charge_counts"));
					// 弹幕量
					String barrage_counts = isKong(json.getString("barrage_counts")) ? "null"
							: getCount(json.getString("barrage_counts"));
					// 匹配上输出value，isdisplay标识为1，否则isdisplay标识为0
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
					output = new Text(site_name + TAB + artist_name + TAB + update_time + TAB + mv_name + TAB
							+ play_counts + TAB + collect_counts + TAB + fav_counts + TAB + download_counts + TAB
							+ publish_time + TAB + comment_counts + TAB + mv_id + TAB + style_classify + TAB
							+ reward_counts + TAB + introduce + TAB + publish_name + TAB + publish_id + TAB + sub_counts
							+ TAB + fans_counts + TAB + charge_counts + TAB + barrage_counts + TAB + isdisplay + TAB
							+ artist_note);

					if (output.toString().split(TAB)[0].equals("letv")) {
						mos.write("letv", output, new Text(), "product=letv/");
					} else if (output.toString().split(TAB)[0].equals("qq_mv")) {
						mos.write("qq", output, new Text(), "product=qq/");
					} else if (output.toString().split(TAB)[0].equals("xiami_mv")) {
						mos.write("xiami", output, new Text(), "product=xiami/");
					}
				}
				// 处理爱奇艺
				else if (json.getString("site_name").matches(".*aiqiyi.*")) {

					// 艺人名
					String artist_name = isKong(json.getString("artist_name")) ? "null"
							: getName(json.getString("artist_name"));
					// 更新时间
					String update_time = isKong(json.getString("update_time")) ? "null"
							: getCommon(json.getString("update_time"));
					// mv名称
					String mv_name = isKong(json.getString("mv_name")) ? "null" : getCommon(json.getString("mv_name"));
					// 播放量
					String play_counts = isKong(json.getString("play_counts")) ? "null"
							: ToolUtils.FormatNumber(getCount(json.getString("play_counts")));
					// 收藏量
					String collect_counts = isKong(json.getString("collect_counts")) ? "null"
							: getCount(json.getString("collect_counts"));
					// 点赞量
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: getCount(json.getString("fav_counts"));
					// 下载量
					String download_counts = isKong(json.getString("download_counts")) ? "null"
							: getCount(json.getString("download_counts"));
					// 发布时间
					String publish_time = isKong(json.getString("publish_time")) ? "null"
							: getAiqiyiTime(json.getString("publish_time"));
					// 评论量
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getCount(json.getString("comment_counts"));
					// MV_ID
					String mv_id = isKong(json.getString("mv_id")) ? "null" : getCommon(json.getString("mv_id"));
					// 分类
					String style_classify = isKong(json.getString("style_classify")) ? "null"
							: getCommon(json.getString("style_classify"));
					// 投蕉量、硬币量
					String reward_counts = isKong(json.getString("reward_counts")) ? "null"
							: getCount(json.getString("reward_counts"));
					// 简介
					String introduce = isKong(json.getString("introduce")) ? "null"
							: getCommon(json.getString("introduce")).replace(",", "").trim();
					// 发布者
					String publish_name = isKong(json.getString("publish_name")) ? "null"
							: getCommon(json.getString("publish_name"));
					// 发布者id
					String publish_id = isKong(json.getString("publish_id")) ? "null"
							: getCommon(json.getString("publish_id"));
					// 投稿数
					String sub_counts = isKong(json.getString("sub_counts")) ? "null"
							: getCount(json.getString("sub_counts"));
					// 发布粉丝数
					String fans_counts = isKong(json.getString("fans_counts")) ? "null"
							: getCount(json.getString("fans_counts"));
					// 充电人数
					String charge_counts = isKong(json.getString("charge_counts")) ? "null"
							: getCount(json.getString("charge_counts"));
					// 弹幕量
					String barrage_counts = isKong(json.getString("barrage_counts")) ? "null"
							: getCount(json.getString("barrage_counts"));
					// 匹配上输出value，isdisplay标识为1，否则isdisplay标识为0
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
					output = new Text(site_name + TAB + artist_name + TAB + update_time + TAB + mv_name + TAB
							+ play_counts + TAB + collect_counts + TAB + fav_counts + TAB + download_counts + TAB
							+ publish_time + TAB + comment_counts + TAB + mv_id + TAB + style_classify + TAB
							+ reward_counts + TAB + introduce + TAB + publish_name + TAB + publish_id + TAB + sub_counts
							+ TAB + fans_counts + TAB + charge_counts + TAB + barrage_counts + TAB + isdisplay + TAB
							+ artist_note);
					if (output.toString().split(TAB)[0].equals("aiqiyi_mv")) {
						mos.write("aiqiyi", output, new Text(), "product=aiqiyi/");
					}
				}
				// 处理b站
				else if (json.getString("site_name").matches(".*bilibili.*")) {
					// 艺人名
					String artist_name = isKong(json.getString("artist_name")) ? "null"
							: getName(json.getString("artist_name"));

					// 更新时间
					String update_time = isKong(json.getString("update_time")) ? "null"
							: getCommon(json.getString("update_time"));
					// mv名称
					String mv_name = isKong(json.getString("mv_name")) ? "null" : getCommon(json.getString("mv_name"));

					String play_counts = isKong(json.getString("play_counts")) ? "null"
							: getCount(json.getString("play_counts"));
					// 收藏量
					String collect_counts = isKong(json.getString("collect_counts")) ? "null"
							: getCount(json.getString("collect_counts"));
					// 点赞量
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: getCount(json.getString("fav_counts"));
					// 下载量
					String download_counts = isKong(json.getString("download_counts")) ? "null"
							: getCount(json.getString("download_counts"));
					// 发布时间
					String publish_time = isKong(json.getString("publish_time")) ? "null"
							: getCommon(json.getString("publish_time"));
					// 评论量
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getCount(json.getString("comment_counts"));
					// MV_ID
					String mv_id = isKong(json.getString("mv_id")) ? "null" : getCommon(json.getString("mv_id"));
					// 分类
					String style_classify = isKong(json.getString("style_classify")) ? "null"
							: getCommon(json.getString("style_classify"));
					// mv名称遍历艺人匹配文件，若mv_name中包含此艺人名，则显示主要艺人名
					int len = 0;
					for (Iterator<Entry<String, List<String>>> i = map.entrySet().iterator(); i.hasNext();) {
						Entry<String, List<String>> e = i.next();
						if (style_classify.toLowerCase().contains(e.getKey())) { //&&e.getKey().length() >= len
							//粗略优化，若有更长字符串匹配，则用更长字符串替代，否则不予替代，最终匹配上最长字符串
								//len = e.getKey().length();
								artist_name = e.getValue().get(0);
								// break;
						}
					}
					// 投蕉量、硬币量
					String reward_counts = isKong(json.getString("reward_counts")) ? "null"
							: getCount(json.getString("reward_counts"));
					// 简介
					String introduce = isKong(json.getString("introduce")) ? "null"
							: getCommon(json.getString("introduce"));
					// 发布者
					String publish_name = isKong(json.getString("publish_name")) ? "null"
							: getCommon(json.getString("publish_name"));
					// 发布者id
					String publish_id = isKong(json.getString("publish_id")) ? "null"
							: getCommon(json.getString("publish_id"));
					// 投稿数
					String sub_counts = isKong(json.getString("sub_counts")) ? "null"
							: getBilibiliTougao(json.getString("sub_counts"));
					// 发布粉丝数
					String fans_counts = isKong(json.getString("fans_counts")) ? "null"
							: getBilibiliFans(json.getString("fans_counts"));
					// 充电人数
					String charge_counts = isKong(json.getString("charge_counts")) ? "null"
							: getCount(json.getString("charge_counts"));
					// 弹幕量
					String barrage_counts = isKong(json.getString("barrage_counts")) ? "null"
							: getCount(json.getString("barrage_counts"));
					// 匹配上输出value，isdisplay标识为1，否则isdisplay标识为0
					String isdisplay = "null";
					String artist_note = "null";
					String artist_name_lower = artist_name.toLowerCase();
					if (map != null || map.isEmpty() == false) {
						if (map.get(artist_name_lower) != null) {
							// artist_name = map.get(artist_name_lower).get(0);
							isdisplay = "1";
							artist_note = map.get(artist_name_lower).get(1);
						} else {
							isdisplay = "0";
						}
					}
					output = new Text(site_name + TAB + artist_name + TAB + update_time + TAB + mv_name + TAB
							+ play_counts + TAB + collect_counts + TAB + fav_counts + TAB + download_counts + TAB
							+ publish_time + TAB + comment_counts + TAB + mv_id + TAB + style_classify + TAB
							+ reward_counts + TAB + introduce + TAB + publish_name + TAB + publish_id + TAB + sub_counts
							+ TAB + fans_counts + TAB + charge_counts + TAB + barrage_counts + TAB + isdisplay + TAB
							+ artist_note);
					if (output.toString().split(TAB)[0].equals("bilibili")) {
						mos.write("bilibili", output, new Text(), "product=bilibili/");
					}
				}
				// 处理A站
				else if (json.getString("site_name").matches(".*acfun.*")) {
					// 艺人名
					String artist_name = isKong(json.getString("artist_name")) ? "null"
							: getName(json.getString("artist_name"));
					// 更新时间
					String update_time = isKong(json.getString("update_time")) ? "null"
							: getCommon(json.getString("update_time"));
					// mv名称
					String mv_name = isKong(json.getString("mv_name")) ? "null" : getCommon(json.getString("mv_name"));

					// 播放量
					String play_counts = isKong(json.getString("play_counts")) ? "null"
							: getCount(json.getString("play_counts"));
					// 收藏量
					String collect_counts = isKong(json.getString("collect_counts")) ? "null"
							: getCount(json.getString("collect_counts"));
					// 点赞量
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: getCount(json.getString("fav_counts"));
					// 下载量
					String download_counts = isKong(json.getString("download_counts")) ? "null"
							: getCount(json.getString("download_counts"));
					// 发布时间
					String publish_time = isKong(json.getString("publish_time")) ? "null"
							: getCommon(json.getString("publish_time"));
					// 评论量
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getCount(json.getString("comment_counts"));
					// MV_ID
					String mv_id = isKong(json.getString("mv_id")) ? "null" : getCommon(json.getString("mv_id"));
					// 分类
					String style_classify = isKong(json.getString("style_classify")) ? "null"
							: getCommon(json.getString("style_classify"));
					// mv名称遍历艺人匹配文件，若mv_name中包含此艺人名，则显示主要艺人名
					int len = 0;
					for (Iterator<Entry<String, List<String>>> i = map.entrySet().iterator(); i.hasNext();) {
						Entry<String, List<String>> e = i.next();
						if (style_classify.toLowerCase().contains(e.getKey())) { //&&e.getKey().length() >= len
							//粗略优化，若有更长字符串匹配，则用更长字符串替代，否则不予替代，最终匹配上最长字符串
								//len = e.getKey().length();
								artist_name = e.getValue().get(0);
								// break;
						}
					}
					// 投蕉量、硬币量
					String reward_counts = isKong(json.getString("reward_counts")) ? "null"
							: getCount(json.getString("reward_counts"));
					// 简介
					String introduce = isKong(json.getString("introduce")) ? "null"
							: getCommon(json.getString("introduce"));
					// 发布者
					String publish_name = isKong(json.getString("publish_name")) ? "null"
							: getCommon(json.getString("publish_name"));
					// 发布者id
					String publish_id = isKong(json.getString("publish_id")) ? "null"
							: getCommon(json.getString("publish_id"));
					// 投稿数
					String sub_counts = isKong(json.getString("sub_counts")) ? "null"
							: getCount(json.getString("sub_counts"));
					// 发布粉丝数
					String fans_counts = isKong(json.getString("fans_counts")) ? "null"
							: getCount(json.getString("fans_counts"));
					// 充电人数
					String charge_counts = isKong(json.getString("charge_counts")) ? "null"
							: getCount(json.getString("charge_counts"));
					// 弹幕量
					String barrage_counts = isKong(json.getString("barrage_counts")) ? "null"
							: getCount(json.getString("barrage_counts"));
					// 匹配上输出value，isdisplay标识为1，否则isdisplay标识为0
					String isdisplay = "null";
					String artist_note = "null";
					String artist_name_lower = artist_name.toLowerCase();
					if (map != null || map.isEmpty() == false) {
						if (map.get(artist_name_lower) != null) {
							// artist_name = map.get(artist_name_lower).get(0);
							isdisplay = "1";
							artist_note = map.get(artist_name_lower).get(1);
						} else {
							isdisplay = "0";
						}
					}
					output = new Text(site_name + TAB + artist_name + TAB + update_time + TAB + mv_name + TAB
							+ play_counts + TAB + collect_counts + TAB + fav_counts + TAB + download_counts + TAB
							+ publish_time + TAB + comment_counts + TAB + mv_id + TAB + style_classify + TAB
							+ reward_counts + TAB + introduce + TAB + publish_name + TAB + publish_id + TAB + sub_counts
							+ TAB + fans_counts + TAB + charge_counts + TAB + barrage_counts + TAB + isdisplay + TAB
							+ artist_note);
					if (output.toString().split(TAB)[0].equals("acfun")) {
						mos.write("acfun", output, new Text(), "product=acfun/");
					}
				}
				// 处理music_station
				else if (json.getString("site_name").matches(".*music_station.*")) {

					// 艺人名
					String artist_name = isKong(json.getString("artist_name")) ? "null"
							: getName(json.getString("artist_name"));
					// 更新时间
					String update_time = isKong(json.getString("update_time")) ? "null"
							: getCommon(json.getString("update_time"));
					// mv名称
					String mv_name = isKong(json.getString("mv_name")) ? "null" : getCommon(json.getString("mv_name"));
					// 播放量
					String play_counts = isKong(json.getString("play_counts")) ? "null"
							: getCount(json.getString("play_counts"));
					// 收藏量
					String collect_counts = isKong(json.getString("collect_counts")) ? "null"
							: getCount(json.getString("collect_counts"));
					// 点赞量
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: getCount(json.getString("fav_counts"));
					// 下载量
					String download_counts = isKong(json.getString("download_counts")) ? "null"
							: getCount(json.getString("download_counts"));
					// 发布时间
					String publish_time = isKong(json.getString("publish_time")) ? "null"
							: getMusic_stationTime(json.getString("publish_time"));
					// 评论量
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getCount(json.getString("comment_counts"));
					// MV_ID
					String mv_id = isKong(json.getString("mv_id")) ? "null" : getCommon(json.getString("mv_id"));
					// 分类
					String style_classify = isKong(json.getString("style_classify")) ? "null"
							: getCommon(json.getString("style_classify"));
					// 投蕉量、硬币量
					String reward_counts = isKong(json.getString("reward_counts")) ? "null"
							: getCount(json.getString("reward_counts"));
					// 简介
					String introduce = isKong(json.getString("introduce")) ? "null"
							: getCommon(json.getString("introduce"));
					// 发布者
					String publish_name = isKong(json.getString("publish_name")) ? "null"
							: getCommon(json.getString("publish_name"));
					// 发布者id
					String publish_id = isKong(json.getString("publish_id")) ? "null"
							: getCommon(json.getString("publish_id"));
					// 投稿数
					String sub_counts = isKong(json.getString("sub_counts")) ? "null"
							: getCount(json.getString("sub_counts"));
					// 发布粉丝数
					String fans_counts = isKong(json.getString("fans_counts")) ? "null"
							: getCount(json.getString("fans_counts"));
					// 充电人数
					String charge_counts = isKong(json.getString("charge_counts")) ? "null"
							: getCount(json.getString("charge_counts"));
					// 弹幕量
					String barrage_counts = isKong(json.getString("barrage_counts")) ? "null"
							: getCount(json.getString("barrage_counts"));
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
					output = new Text(site_name + TAB + artist_name + TAB + update_time + TAB + mv_name + TAB
							+ play_counts + TAB + collect_counts + TAB + fav_counts + TAB + download_counts + TAB
							+ publish_time + TAB + comment_counts + TAB + mv_id + TAB + style_classify + TAB
							+ reward_counts + TAB + introduce + TAB + publish_name + TAB + publish_id + TAB + sub_counts
							+ TAB + fans_counts + TAB + charge_counts + TAB + barrage_counts + TAB + isdisplay + TAB
							+ artist_note);
					if (output.toString().split(TAB)[0].equals("music_station")) {
						mos.write("musicstation", output, new Text(), "product=music_station/");
					}
				} else if (json.getString("site_name").matches(".*youku.*")) {

					// 艺人名
					String artist_name = isKong(json.getString("artist_name")) ? "null"
							: getName(json.getString("artist_name"));
					// 更新时间
					String update_time = isKong(json.getString("update_time")) ? "null"
							: getCommon(json.getString("update_time"));
					// mv名称
					String mv_name = isKong(json.getString("mv_name")) ? "null" : getCommon(json.getString("mv_name"));
					// 播放量
					String play_counts = isKong(json.getString("play_counts")) ? "null"
							: getCount(json.getString("play_counts"));
					// 收藏量
					String collect_counts = isKong(json.getString("collect_counts")) ? "null"
							: getCount(json.getString("collect_counts"));
					// 点赞量
					String fav_counts = isKong(json.getString("fav_counts")) ? "null"
							: getCount(json.getString("fav_counts"));
					// 下载量
					String download_counts = isKong(json.getString("download_counts")) ? "null"
							: getCount(json.getString("download_counts"));
					// 发布时间
					String publish_time = isKong(json.getString("publish_time")) ? "null"
							: getCommon(json.getString("publish_time"));
					// 评论量
					String comment_counts = isKong(json.getString("comment_counts")) ? "null"
							: getYouku_comment(json.getString("comment_counts"));
					// MV_ID
					String mv_id = isKong(json.getString("mv_id")) ? "null" : getCommon(json.getString("mv_id"));
					// 分类
					String style_classify = isKong(json.getString("style_classify")) ? "null"
							: getCommon(json.getString("style_classify"));
					// 投蕉量、硬币量
					String reward_counts = isKong(json.getString("reward_counts")) ? "null"
							: getCount(json.getString("reward_counts"));
					// 简介
					String introduce = isKong(json.getString("introduce")) ? "null"
							: getCommon(json.getString("introduce"));
					// 发布者
					String publish_name = isKong(json.getString("publish_name")) ? "null"
							: getCommon(json.getString("publish_name"));
					// 发布者id
					String publish_id = isKong(json.getString("publish_id")) ? "null"
							: getCommon(json.getString("publish_id"));
					// 投稿数
					String sub_counts = isKong(json.getString("sub_counts")) ? "null"
							: getCount(json.getString("sub_counts"));
					// 发布粉丝数
					String fans_counts = isKong(json.getString("fans_counts")) ? "null"
							: getCount(json.getString("fans_counts"));
					// 充电人数
					String charge_counts = isKong(json.getString("charge_counts")) ? "null"
							: getCount(json.getString("charge_counts"));
					// 弹幕量
					String barrage_counts = isKong(json.getString("barrage_counts")) ? "null"
							: getCount(json.getString("barrage_counts"));
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
					output = new Text(site_name + TAB + artist_name + TAB + update_time + TAB + mv_name + TAB
							+ play_counts + TAB + collect_counts + TAB + fav_counts + TAB + download_counts + TAB
							+ publish_time + TAB + comment_counts + TAB + mv_id + TAB + style_classify + TAB
							+ reward_counts + TAB + introduce + TAB + publish_name + TAB + publish_id + TAB + sub_counts
							+ TAB + fans_counts + TAB + charge_counts + TAB + barrage_counts + TAB + isdisplay + TAB
							+ artist_note);
					if (output.toString().split(TAB)[0].equals("youku_mv")) {
						mos.write("youku", output, new Text(), "product=youku/");
					}
				}

			}
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(MV.class);
		job.setJobName("MV");
		job.setNumReduceTasks(1);
		job.setMapperClass(MvMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		String artist_isdisplay_table = "/music/warehouse/music_artistmodel_artist_isdiplay/event_day=20170301";
		job.addCacheFile(new URI(artist_isdisplay_table + "/part-m-00000#artist_isdisplay"));
		MultipleOutputs.addNamedOutput(job, "acfun", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "aiqiyi", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "bilibili", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "letv", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "musicstation", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "qq", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "xiami", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "youku", TextOutputFormat.class, Text.class, NullWritable.class);

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
		String HiveTable_out = "/music/warehouse/music_artistmodel_mv/dt=" + Month_firstday;
		Path Hive_path = new Path(HiveTable_out);

		// String time = sdf.format(dBefore); // 格式化前一天
		// String out = "/user/work/ArtistModel/" + time + "/MV";
		// Path path = new Path(out);
		hdfs.delete(Hive_path, true);
		return MV.runLoadMapReducue(conf, args[0], Hive_path);
	}

}
