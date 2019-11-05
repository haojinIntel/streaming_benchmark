package com.intel.streaming_benchmark.common;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

/**
 * Time data format converter
 */
public class DateUtils {
    public static final int dayOfMillis = 86400000;
    public static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String DATEKEY_FORMAT = "yyyyMMdd";

    /**
     * Convert millisecond timestamps into: yyyy-MM-dd HH:mm:ss
     *
     * @param time
     * @return
     */
    public static String parseLong2String(long time) {
        return parseLong2String(time, TIME_FORMAT);
    }

    /**
     *  Convert millisecond timestamps into defined date format
     *
     * @param time
     * @param pattern
     * @return
     */
    public static String parseLong2String(long time, String pattern) {
        return parseLong2String(time, new SimpleDateFormat(pattern));
    }

    /**
     *  Convert millisecond timestamps into date according to formatter
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
     * Convert string time into long timestamps
     *
     * @param date time type，format：yyyy-MM-dd HH:mm:ss
     * @return
     */
    public static long parseString2Long(String date) {
        return parseString2Long(date, TIME_FORMAT);
    }

    /**
     * Convert string time into long timestamps according to the time format string
     *
     * @param date
     * @param pattern
     * @return
     */
    public static long parseString2Long(String date, String pattern) {
        return parseString2Long(date, new SimpleDateFormat(pattern));
    }

    /**
     *  Convert string time into long timestamps according to the time format string
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
     *  Convert long timestamps into the value according to the time type
     *
     * @param millis milliseconds timestamp
     * @param type   time type
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

        throw new IllegalArgumentException("Parameter exception");
    }

    /**
     * get the date of the day，format:yyyy-MM-dd
     *
     * @return Date of the day
     */
    public static String getTodayDate() {
        return new SimpleDateFormat(DATE_FORMAT).format(new Date());
    }

    /**
     * Get a random milliseconds timestamps of today
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
     * Time type
     */
    public static enum DateTypeEnum {
        YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND
    }

    /**
     * Judge if time1 is before time2
     *
     * @param time1
     * @param time2
     * @return Judgement result
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
     * Judge if time1 is after time2
     *
     * @param time1
     * @param time2
     * @return Judgement result
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
     * Calculate time difference(Unit: second)
     *
     * @param time1
     * @param time2
     * @return difference
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
     *get year, month, day and hour
     *
     * @param datetime time（yyyy-MM-dd HH:mm:ss）
     * @return result（yyyy-MM-dd_HH）
     */
    public static String getDateHour(String datetime) {
        String date = datetime.split(" ")[0];
        String hourMinuteSecond = datetime.split(" ")[1];
        String hour = hourMinuteSecond.split(":")[0];
        return date + "_" + hour;
    }

    /**
     * get the date of yesterday（yyyy-MM-dd）
     *
     * @return the date of yesterday
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
     * format date，reserve minute
     * yyyyMMddHHmm
     *
     * @param date
     * @return
     */
    public static String formatTimeMinute(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        return sdf.format(date);
    }

    public static String fileToString(File file) throws Exception{
        FileInputStream inStream = new FileInputStream(file);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        try {

            Boolean reading = true;
            while (reading) {
                int c = inStream.read();
                if(c == -1){
                    reading = false;
                }else{
                    outStream.write(c);
                }
            }
            outStream.flush();
        }catch (Exception e){
            System.err.println(e.getMessage());
        }finally {
            inStream.close();
        }
        return new String(outStream.toByteArray(), "UTF-8");
    }

}
