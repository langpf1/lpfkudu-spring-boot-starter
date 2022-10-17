package org.fiend.kudu.starter.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 日期操作工具集
 */
public abstract class DateUtil {
	private static Logger log = LoggerFactory.getLogger(DateUtil.class);

	public static void main(String[] args) {
		System.out.println(getFutureSysDate("2020-02-29 23:32:33", 24 * 3600 * 1000));
	}

	/** 格式:yyyy-MM-dd HH:mm:ss */
	public static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";

	/** 格式:yyyyMMdd. */
	public static final String DATE_PATTERN_MIN = "yyyyMMdd";

	/** 格式:yyyy-MM-dd. */
	public static final String DATE_PATTERN_STRIKETHROUGH = "yyyy-MM-dd";

	/** 格式:yyyy-MM-dd. */
	public static final String DATE_PATTERN_POINT = "yyyy.MM.dd";

	/**
	 * yyyy-MM-dd --> 6516515
	 */
	public static long str2Long(String dateStr) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN_STRIKETHROUGH);
		long time;

		Date date = sdf.parse(dateStr);
		time = date.getTime();

		return time;
	}

	/**
	 * yyyy.MM.dd --> 6516515
	 */
	public static long str2Long2(String dateStr) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN_POINT);
		long time = 0;

		Date date = sdf.parse(dateStr);
		time = date.getTime();

		return time;
	}

	/**
	 * yyyy-MM-dd --> 6516515
	 */
	public static String Long2str(long time) {
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN_STRIKETHROUGH);
		String dateStr;

		Date date = new Date(time);
		dateStr = sdf.format(date);

		return dateStr;
	}

	/**
	 * 系统时间获取 yyyy-MM-dd HH:mm:ss
	 * @return 系统时间
	 */
	public static String getSystemDate() {
		SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_PATTERN);
		Date systemDate = new Date();

		return dateFormat.format(systemDate);
	}

	/**
	 * 系统时间获取 yyyyMMddHHmmss
	 *
	 * @return 系统时间
	 */
	public static String getStringDate() {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		Date systemDate = new Date();
		return dateFormat.format(systemDate);
	}

	/**
	 * yyyyMMddHHmmss --> yyyy-MM-dd HH:mm:ss
	 *
	 * @param s
	 * @return
	 */
	public static String formatDateStr(String s) {
		if ((s == null) || (s.length() < 14)) {
			return null;
		}
		StringBuffer sb = new StringBuffer();

		sb.append(s.substring(0, 4));
		sb.append("-");
		sb.append(s.substring(4, 6));
		sb.append("-");
		sb.append(s.substring(6, 8));
		sb.append(" ");
		sb.append(s.substring(8, 10));
		sb.append(":");
		sb.append(s.substring(10, 12));
		sb.append(":");
		sb.append(s.substring(12, 14));

		return sb.toString();
	}

	/**
	 * 年月获取 yyyy-MM
	 *
	 * @return 系统年月
	 */
	public static String getSystemDateYM() {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM");
		Date systemDate = new Date();
		return dateFormat.format(systemDate);
	}

	/**
	 * 获取指定格式时间
	 *
	 * @return 系统时间
	 */
	public static String getFormatDate(String format) {

		SimpleDateFormat dateFormat = new SimpleDateFormat(format);
		Date systemDate = new Date();
		return dateFormat.format(systemDate);
	}

	/**
	 * 年月取得 yyyy年MM月
	 *
	 * @return 月
	 */
	public static String getYearMonthSys() {
		String date = getSystemDate();
		return StringUtil.concat(date.substring(0, 4), "年",
				date.substring(4, 6), "月", date.substring(6, 8), "日");
	}

	/**
	 * 年月取得 yyyy年MM月
	 * @return 月
	 */
	public static String getYearSys() {
		String date = getSystemDate();
		return StringUtil.concat(date.substring(0, 4), "年",
				date.substring(4, 6), "月");
	}

	/**
	 * 系统日期的日获取
	 * @return 日
	 */
	public static String getDayOfSys() {
		String date = getSystemDate();
		return date.substring(6, 8);
	}

	/**
	 * 获取系统日期的年月日
	 * @return 2018-03-10
	 */
	public static String getYTDOfSys() {
		String date = getSystemDate();
		return date.substring(0, 10);
	}

	/**
	 * 系统日期的月份获取
	 * @return 月
	 */
	public static String getMonthOfSys() {
		String date = getSystemDate();
		return date.substring(4, 6);
	}

	/**
	 * 系统日期的年份获取
	 * @return 年
	 */
	public static String getYearOfSys() {
		String date = getSystemDate();
		return date.substring(0, 4);
	}

	/**
	 * 文字日期(yyyy-MM-dd HH:mm:ss)->Date日期转换
	 *
	 * @param sDate
	 *            文字日期(yyyy-MM-dd HH:mm:ss)
	 * @return 日期
	 */
	public static Date toDate(final String sDate) {
		SimpleDateFormat sm = new SimpleDateFormat(DATE_PATTERN, Locale.getDefault());

		try {
			return sm.parse(sDate);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Date日期 ->文字列日期(yyyyMMddHHmmss)转换
	 *
	 * @param date
	 *            Date日期
	 * @return 文字列日期(yyyyMMddHHmmss)
	 */
	public static String toString(final Date date) {
		if (date == null) {
			return "";
		}
		SimpleDateFormat sm = new SimpleDateFormat(DATE_PATTERN,
				Locale.getDefault());
		return sm.format(date);
	}

	/**
	 * 年月取得 yyyy年MM月
	 *
	 * @param date
	 * @return 月
	 */
	public static String getYearMonthDay(String date) {
		if (date == null) {
			return "";
		}

		return StringUtil.concat(date.substring(0, 4), "年",
				date.substring(4, 6), "月", date.substring(6, 8), "日");
	}

	/**
	 * 年月取得 yyyy年MM月
	 *
	 * @param date
	 * @return 月
	 */
	public static String getYearMonth(String date) {
		if (date == null) {
			return "";
		}

		return StringUtil.concat(date.substring(0, 4), "年",
				date.substring(4, 6), "月");
	}

	/**
	 * 指定日期的日获取
	 *
	 * @param date
	 *            日期
	 * @return 日
	 */
	public static String getDayOfDate(String date) {
		return date.substring(6, 8);
	}
	/**
	 * 指定日期的月份获取
	 *
	 * @param date
	 *            日期
	 * @return 月
	 */
	public static String getMonthOfDate(String date) {
		return date.substring(4, 6);
	}

	/**
	 * 指定日期的年份获取
	 *
	 * @param date
	 *            日期
	 * @return 年
	 */
	public static String getYearOfDate(String date) {
		return date.substring(0, 4);
	}

	/**
	 * 日期字符格式化
	 *
	 * @param date
	 * @return 日期 yyyy年MM月dd日HH:mm
	 */
	public static String format(String date) {
		return StringUtil.concat(date.substring(0, 4), "年",
				date.substring(4, 6), "月", date.substring(6, 8), "日",
				date.substring(8, 10), ":", date.substring(10, 12));
	}

	/**
	 * 检查是否过期
	 *
	 * @param outTime
	 *            过期时间
	 * @return false:没有过期，true 过期
	 */
	public static boolean checkOutTime(String outTime) {
		SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_PATTERN_MIN);
		Date systemDate = new Date();
		try {
			if (outTime == null) {
				return false; //
			}
			return dateFormat.parse(outTime.substring(0, 8)).before(systemDate)
					&& !dateFormat.parse(outTime.substring(0, 8)).equals(
					dateFormat.parse(dateFormat.format(systemDate)));
		} catch (ParseException e) {
			log.error(e.getMessage());
			e.printStackTrace();
			return true;
		}
	}

	/**
	 * 获取过去第几天的日期
	 *
	 * @param past
	 * @return 返回格式		yyyy-MM-dd
	 */
	public static String getPastDate(int past) {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.DAY_OF_YEAR, calendar.get(Calendar.DAY_OF_YEAR) - past);
		Date today = calendar.getTime();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		String result = format.format(today);

		return result;
	}

	/**
	 * 获取过去第几天的日期
	 *
	 * @param past
	 * @return 返回格式		yyyy-MM-dd HH:mm:ss
	 */
	public static String getPastSysDate(int past) {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.DAY_OF_YEAR, calendar.get(Calendar.DAY_OF_YEAR) - past);
		Date today = calendar.getTime();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String result = format.format(today);

		return result;
	}

	/**
	 * 获取过去第几天的日期
	 *
	 * @param past
	 * @return 返回格式		yyyyMMddHHmmss
	 */
	public static String getPastStrDate(int past) {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.DAY_OF_YEAR, calendar.get(Calendar.DAY_OF_YEAR) - past);
		Date today = calendar.getTime();
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
		String result = format.format(today);

		return result;
	}

	/**
	 * 获取数月之后的日期
	 *
	 * @param num
	 *            月数
	 * @return
	 */
	public static String getNextMountDate(int num) {

		SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_PATTERN);
		Date systemDate = new Date();
		Calendar c = Calendar.getInstance();
		c.setTime(systemDate);
		c.add(Calendar.MONTH, num);
		return dateFormat.format(c.getTime());
	}

	/**
	 * 获取过去第几天的日期
	 *
	 * @param dateStr				"2020-05-16 23:12:09"
	 * @param futureMilliSecTime	54545545545
	 * @return 返回格式		yyyy-MM-dd HH:mm:ss
	 */
	public static String getFutureSysDate(String dateStr, int futureMilliSecTime) {
		if (!isValidDate(dateStr)) {
			return null;
		}
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN, Locale.getDefault());
		Date baseDate = null;
		try {
			baseDate = sdf.parse(dateStr);
		} catch (ParseException e) {
			log.error(e.getMessage());
			e.printStackTrace();
			return null;
		}

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(baseDate);
		calendar.add(Calendar.MILLISECOND, futureMilliSecTime);

		// calendar.set(Calendar.DAY_OF_YEAR, calendar.get(Calendar.DAY_OF_YEAR) - past);
		Date futureDate = calendar.getTime();

		return sdf.format(futureDate);
	}

	/**
	 * @param startTime 2018-1-19
	 * @param endTime 2018-1-19
	 * @return list [2018-1-19 22:17:51, 2018-1-19 22:17:54]
	 */
	public static List<String> getFormatTime(String startTime, String endTime) {
		List<String> timeList = new ArrayList<>();

		if (startTime == null || "".equals(startTime) || endTime == null || "".equals(endTime)) {
			startTime = DateUtil.getPastSysDate(7);
			endTime = DateUtil.getSystemDate();

			timeList.add(startTime);
			timeList.add(endTime);

			return timeList;
		}

		if (startTime.compareTo(endTime) > 0) {
			String t;
			t = startTime;
			startTime = endTime;
			endTime = t;
		}

		startTime += " 00:00:00";
		endTime += " 23:59:59";

		timeList.add(startTime);
		timeList.add(endTime);
		return timeList;
	}

	public static boolean isValidDate(String str) {
		boolean convertSuccess = true;

		SimpleDateFormat format = new SimpleDateFormat(DATE_PATTERN);
		try {
			// 设置lenient为false. 否则SimpleDateFormat会比较宽松地验证日期，比如2007/02/29会被接受，并转换成2007/03/01
			format.setLenient(false);
			format.parse(str);
		} catch (ParseException e) {
			// e.printStackTrace();
			// 如果throw java.text.ParseException或者NullPointerException，就说明格式不对
			convertSuccess = false;
		}

		return convertSuccess;
	}
}
