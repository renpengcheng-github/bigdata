package Spider;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ToolUtils {

	public static String IdToSite(String id) {
		String result = id;
		if (id.equals("1")) {
			result = "爱奇艺";
		} else if (id.equals("2")) {
			result = "腾讯视频";
		} else if (id.equals("3")) {
			result = "虾米音乐";
		} else if (id.equals("4")) {
			result = "永乐票务";
		} else if (id.equals("5")) {
			result = "大麦网";
		} else if (id.equals("6")) {
			result = "中国票房";
		} else if (id.equals("7")) {
			result = "百度贴吧";
		}
		return result;
	}

	// "artist_name": ["Royce Da 5'9\""] \置空格 / , 不管
	public static String getName(String value) {
		if (value != null) {
			return value.replace("\"", "").replace("[", "").replace("]", "").replace("\n", "")
					.replace("\r", "").replace("\\n", "").replace("\\r", "").replace("//", "").replace(" / ", "").replace("\\", " ")
					.trim();
		} else {
			return null;
		}

	}
	//20130916：汪小菲
	//20130916：
	//20130916 汪小菲
	//汪小菲
	//：汪小菲
	//这五种情况可以取得数字 或者 字符
	public static String getName_Time(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("^([0-9]*)(:|：){0,1}(.*?)$");
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
	public static String getTime_Name(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("^([0-9]*)(:|：){0,1}(.*?)$");
			Matcher m = p.matcher(value);
			String result = null;
			while (m.find()) {
				result = m.group(1);
			}
			return FormatDate(result);
		} else {
			return null;
		}
	}
	//工具类里getcount主要处理FormatNumber之后带逗号的情况
	public static String getCount(String value) {
		if (value != null) {
			return value.replace(",", "").trim();
		} else {
			return null;
		}

	}
	//带getcount解决乘完之后带逗号问题（，）
	public static String FormatNumber(String value) {
		// 字符串格式：数字+万|亿
		if (value == null || value.trim().isEmpty()) {
			value = "0";
		} else if (value.matches("^[0-9\\.]+(万|亿)?$")) {
			java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
			if (value.contains("万")) {
				value = nf.format(Double.parseDouble(value.substring(0, value.length() - 1)) * 10000);
			} else if (value.contains("亿")) {
				value = nf.format(Double.parseDouble(value.substring(0, value.length() - 1)) * 100000000);
			}
		}
		return getCount(value);
	}

	// 20170101 转化为2017-01-01    2017.01.01转化为20170101
		public static String FormatDate(String date) {
			// 规范日期格式xxxx-xx-xx
			String time="";
		if (date == null || date.trim().isEmpty()) {
			return null;
		}
		else
		{
			try {
				SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd"); //日期形式
				Date f=sdf.parse(date.replace(".", "").trim()); //以上面的日期形式截取string  xxxxxxxx 转化为日期型
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");//日期形式2
			    time=formatter.format(f);  //将上面的日期型数据f 以formatter 日期形式转化

			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		 }
			return time;
		
		}

	public static String FormatUnixTime(String time) {
		// 时间戳转化成xxxx-xx-xx
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		return time.matches("^[0-9]+$") ? sdf.format(new Date(Long.parseLong(time) * 1000)) : "0000-00-00";
	}

	// xxxx年xx月（xx日）显示成xxxx-xx-xx
	public static String FormatStringTime(String value) {
		if (value != null) {
			Pattern p = Pattern.compile("([0-9]+)年([0-9]*)月([0-9]*)(日){0,1}");
			Matcher m = p.matcher(value);
			String year = "";
			String month = "";
			String day = "";
			// String daytime="";
			while (m.find()) {
				year = m.group(1);
				month = m.group(2);
				day = m.group(3);
				if (day.isEmpty()) {
					day = "";
				} else {
					day = "-" + day;
				}
			}
			return year + "-" + month + day;
		} else {
			return null;
		}
	}

	public static String FormatString(String str) {
		// 规范空字符串
		if (str.trim().isEmpty()) {
			return "NULL";
		} else {
			return str.trim().replace("&#183;", "·");
		}
	}
	
	
	//解决   ,name1:,,name2：,,,name3的情况         可以得到 name1,name2,name3 ()
	public static String getsplit_name(String value){
		if(value!=null){
		List<String> all=new ArrayList<String>();
		String a[] = value.replace(":", "").replace("：", "").split(","); 
		String outputs=""; 
		for(int i=0;i<a.length;i++){
			if (a[i].trim().isEmpty()){
				continue;
			}
			else
			{all.add(a[i]);}
		}
		outputs=String.join(",", all);
		return outputs;
		}
		else{return null;}
		
	}
}