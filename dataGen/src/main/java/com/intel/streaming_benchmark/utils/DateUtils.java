package com.intel.streaming_benchmark.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

/**
 * 时间相关数据格式化工具器
 */
public class DateUtils {
    public static final int dayOfMillis = 86400000;
    public static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String DATEKEY_FORMAT = "yyyyMMdd";

    /**
     * 按照给定的毫秒级时间戳进行时间数据格式化，返回结果默认格式为：yyyy-MM-dd HH:mm:ss
     *
     * @param time
     * @return
     */
    public static String parseLong2String(long time) {
        return parseLong2String(time, TIME_FORMAT);
    }

    /**
     * 按照给定的毫秒级时间戳以及格式化字符串进行日期数据格式化
     *
     * @param time
     * @param pattern
     * @return
     */
    public static String parseLong2String(long time, String pattern) {
        return parseLong2String(time, new SimpleDateFormat(pattern));
    }

    /**
     * 按照给定的毫秒级时间戳以及格式化器进行日期数据格式化
     *
     * @param time
     * @param sdf
     * @return
     */
    public static String parseLong2String(long time, SimpleDateFormat sdf) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time);
        return sdf.format(cal.getTime());
    }

    /**
     * 将传入的特定格式的字符串时间转换为long类型的时间戳值
     *
     * @param date 时间类型，格式为：yyyy-MM-dd HH:mm:ss
     * @return
     */
    public static long parseString2Long(String date) {
        return parseString2Long(date, TIME_FORMAT);
    }

    /**
     * 通过给定的字符格式的时间和时间格式化字符串，将时间数据转换为long类型的值
     *
     * @param date
     * @param pattern
     * @return
     */
    public static long parseString2Long(String date, String pattern) {
        return parseString2Long(date, new SimpleDateFormat(pattern));
    }

    /**
     * 通过给定的字符格式的时间和时间格式化器，将时间数据转换为long类型的值
     *
     * @param date
     * @param sdf
     * @return
     */
    public static long parseString2Long(String date, SimpleDateFormat sdf) {
        try {
            return sdf.parse(date).getTime();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据传入的毫秒级时间戳和时间类型，获取对应的值
     *
     * @param millis 毫秒级时间戳
     * @param type   时间类型
     * @return
     */
    public static int getSpecificDateValueOfDateTypeEnum(long millis, DateTypeEnum type) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(millis);
        switch (type) {
            case YEAR:
                return cal.get(Calendar.YEAR);
            case MONTH:
                return cal.get(Calendar.MONTH) + 1;
            case DAY:
                return cal.get(Calendar.DAY_OF_MONTH);
            case HOUR:
                return cal.get(Calendar.HOUR_OF_DAY);
            case MINUTE:
                return cal.get(Calendar.MINUTE);
            case SECOND:
                return cal.get(Calendar.SECOND);
            case MILLISECOND:
                return cal.get(Calendar.MILLISECOND);
        }

        throw new IllegalArgumentException("参数异常<这个异常不应该产生的....>");
    }

    /**
     * 获取当天的日期，格式为:yyyy-MM-dd
     *
     * @return 当天日期
     */
    public static String getTodayDate() {
        return new SimpleDateFormat(DATE_FORMAT).format(new Date());
    }

    /**
     * 获取一个随机的当天的毫秒级时间戳值，根据给定的随机对象
     *
     * @param random
     * @return
     */
    public static long getRandomTodayTimeOfMillis(Random random) {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        if (random.nextDouble() <= 0.7) {
            // [0-21] => 70%
            int millis = dayOfMillis / 8 * 7;
            cal.add(Calendar.MILLISECOND, 1 + random.nextInt(millis));
        } else {
            // [1-23] => 30%
            int millis = dayOfMillis / 24;
            cal.add(Calendar.MILLISECOND, millis + random.nextInt(millis * 23));
        }
        return cal.getTimeInMillis();
    }

    /**
     * 时间类型
     */
    public static enum DateTypeEnum {
        YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND
    }

    /**
     * 判断一个时间是否在另一个时间之前
     *
     * @param time1 第一个时间
     * @param time2 第二个时间
     * @return 判断结果
     */
    public static boolean before(String time1, String time2) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);
            Date dateTime1 = sdf.parse(time1);
            Date dateTime2 = sdf.parse(time2);

            if (dateTime1.before(dateTime2)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 判断一个时间是否在另一个时间之后
     *
     * @param time1 第一个时间
     * @param time2 第二个时间
     * @return 判断结果
     */
    public static boolean after(String time1, String time2) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);
            Date dateTime1 = sdf.parse(time1);
            Date dateTime2 = sdf.parse(time2);

            if (dateTime1.after(dateTime2)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 计算时间差值（单位为秒）
     *
     * @param time1 时间1
     * @param time2 时间2
     * @return 差值
     */
    public static int minus(String time1, String time2) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);
            Date datetime1 = sdf.parse(time1);
            Date datetime2 = sdf.parse(time2);

            long millisecond = datetime1.getTime() - datetime2.getTime();

            return Integer.valueOf(String.valueOf(millisecond / 1000));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取年月日和小时
     *
     * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
     * @return 结果（yyyy-MM-dd_HH）
     */
    public static String getDateHour(String datetime) {
        String date = datetime.split(" ")[0];
        String hourMinuteSecond = datetime.split(" ")[1];
        String hour = hourMinuteSecond.split(":")[0];
        return date + "_" + hour;
    }

    /**
     * 获取昨天的日期（yyyy-MM-dd）
     *
     * @return 昨天的日期
     */
    public static String getYesterdayDate() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DAY_OF_YEAR, -1);

        Date date = cal.getTime();

        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        return sdf.format(date);
    }

    /**
     * 格式化时间，保留到分钟级别
     * yyyyMMddHHmm
     *
     * @param date
     * @return
     */
    public static String formatTimeMinute(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        return sdf.format(date);
    }

}
