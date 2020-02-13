/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.storm.jdbc.util;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * 날짜와 시간에 관련 Utility 클래스
 *
 * @author bkwon
 */
public class DateTimeUtil {

    public static final String DEFAULT_DATE_PT = "yyyyMMddHHmmss";
    public static final String ZERRO_TIME_HOUR_PT = "yyyyMMddHH0000";
    public static final String ZERRO_TIME_MIN_PT = "yyyyMMddHHmm00";
    public static final String ZERRO_TIME_PT = "yyyyMMdd000000";
    public static final String KOR_DATE_PT = "yyyy.MM.dd HH:mm:ss";
    private static final Logger log = Logger.getLogger(DateTimeUtil.class);

    /**
     * YYYYMMDDhhmmss' in the form of 14-digit date string SimpleDateFomat converted to a format
     * that supports them.
     *
     * @param fixedDate 'YYYYMMDDhhmmss' , Must be 14 length.
     * @param pattern   format pattern
     * @return formated string
     */
    public static String getFixedSimpleDateFormat(String fixedDate, String pattern) {
        if (fixedDate == null || fixedDate.length() < 14) {
            return fixedDate;
        }
        int year = Integer.valueOf(fixedDate.substring(0, 4));
        int month = Integer.valueOf(fixedDate.substring(4, 6)) - 1;  // calendar  +1
        int day = Integer.valueOf(fixedDate.substring(6, 8));
        int hh = Integer.valueOf(fixedDate.substring(8, 10));
        int mm = Integer.valueOf(fixedDate.substring(10, 12));
        int ss = Integer.valueOf(fixedDate.substring(12, 14));

        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(year, month, day, hh, mm, ss);
        FastDateFormat ff = FastDateFormat.getInstance(pattern);
        return ff.format(c);
    }

    /**
     * 현재시간을 구한다
     *
     * @param pattern 패턴 문자열이 없으면 기본 패턴 "yyyyMMddHHmmss" 을 사용
     * @return yyyyMMddHHmmss 형태의 날짜시간 문자열
     */
    public static String getNowSimpleDateFormat(String pattern) {
        String p = DateTimeUtil.DEFAULT_DATE_PT;
        if (pattern != null) {
            p = pattern;
        }
        Calendar c = Calendar.getInstance();
        FastDateFormat ff = FastDateFormat.getInstance(p);
        return ff.format(c);
    }

    public static String getNowSimpleDateFormat(TimeZone tz, String pattern) {
        Calendar c = Calendar.getInstance(tz);
        FastDateFormat ff = FastDateFormat.getInstance(pattern);
        return ff.format(c);
    }

    public static String getSimpleDateFormat(Calendar c, String pattern) {
        FastDateFormat ff = FastDateFormat.getInstance(pattern);
        return ff.format(c);
    }

    /**
     * yyyyMMddHHmmss 형태의 현재 날짜을 얻는다
     *
     * @return
     */
    public static String getNow() {
        return getNowSimpleDateFormat(null);
    }

    /**
     * Date 값을 String으로 변환
     *
     * @param date
     * @param pattern
     * @return
     */
    public static String getDatetoStr(Date date, String pattern) {
        Calendar c = Calendar.getInstance();
        c.clear();
        c.setTimeInMillis(date.getTime());
        return DateTimeUtil.getSimpleDateFormat(c, pattern);
    }

    public static String getDatetoStr(Date date) {
        return DateTimeUtil.getDatetoStr(date, DateTimeUtil.DEFAULT_DATE_PT);
    }

    public static String getDatetoStr(Long mill) {
        return getDatetoStr(mill, DateTimeUtil.DEFAULT_DATE_PT);
    }

    public static String getDatetoStr(Long mill, String pattern) {
        Calendar c = Calendar.getInstance();
        c.clear();
        c.setTimeInMillis(mill);
        return DateTimeUtil.getSimpleDateFormat(c, pattern);
    }

    /**
     * String 포맷을 Date형으로 변환 , 반드시 yyyy-MM-dd 형태의 문자열
     *
     * @param sDate
     * @return
     */
    public static Date getStrToDate(String sDate) {
        String pattern = "yyyy-MM-dd";
        return DateTimeUtil.getStrToDate(sDate, pattern);

    }

    public static Date getStrToDate(String sDate, String pattern) {
        SimpleDateFormat sm = new SimpleDateFormat(pattern);
        try {
            return sm.parse(sDate);
        } catch (ParseException pe) {
            log.error(pe);
        }
        return null;
    }

    /**
     * String 포맷을 Date형으로 변환 , 반드시 yyyy-MM-dd HH:mm:ss 형태의 문자열
     *
     * @param sDateTime
     * @return
     */
    public static Date getStrToDateTime(String sDateTime) {
        return DateTimeUtil.getStrToDateTime(sDateTime, "yyyy-MM-dd HH:mm:ss");
    }

    public static Date getStrToDateTime(String sDateTime, String pattern) {
        String defaultPattern = "yyyy-MM-dd HH:mm:ss";
        if (pattern != null) {
            defaultPattern = pattern;
        }
        try {
            SimpleDateFormat sm = new SimpleDateFormat(defaultPattern);
            return sm.parse(sDateTime);
        } catch (ParseException pe) {
            log.error(pe);
        }
        return null;
    }

    public static long getLongToDateTime(String sDateTime, String pattern) {
        Date d = DateTimeUtil.getStrToDateTime(sDateTime, pattern);
        return d.getTime();

    }

    /**
     * 주어진 날짜를 기준으로 날짜를 + , - 한다. 하루전 날짜를 원할경우 -1,
     *
     * @param date
     * @param amount
     * @return
     */
    public static Date getChangeDay(Date date, int amount) {
        return DateTimeUtil.getChangeCalendar(date, Calendar.DAY_OF_MONTH, amount);
    }

    public static String getChangeDayStr(Date date, int amount) {
        return DateTimeUtil.getChangeCalendar(date.getTime(), Calendar.DAY_OF_MONTH, amount, DateTimeUtil.DEFAULT_DATE_PT);
    }

    /**
     * 주어진 날짜를 기준으로 시간를 + , - 한다. 한시간전 날짜를 원할경우 -1,
     *
     * @param date
     * @param amount
     * @return
     */
    public static Date getChangeHour(Date date, int amount) {
        return DateTimeUtil.getChangeCalendar(date, Calendar.HOUR_OF_DAY, amount);
    }

    public static String getChangeHourStr(Date date, int amount) {
        return DateTimeUtil.getChangeCalendar(date.getTime(), Calendar.HOUR_OF_DAY, amount, DateTimeUtil.DEFAULT_DATE_PT);
    }

    public static String getChangeHourStr(Date date, int amount, String pattern) {
        return DateTimeUtil.getChangeCalendar(date.getTime(), Calendar.HOUR_OF_DAY, amount, pattern);
    }

    /**
     * 분을 변경한다.
     *
     * @param date
     * @param amount
     * @return
     */
    public static Date getChangeMinute(Date date, int amount) {
        return DateTimeUtil.getChangeCalendar(date, Calendar.MINUTE, amount);
    }

    public static String getChangeMinuteStr(Date date, int amount) {
        return DateTimeUtil.getChangeCalendar(date.getTime(), Calendar.MINUTE, amount, DateTimeUtil.DEFAULT_DATE_PT);
    }

    /**
     * 초를 변경한다.
     *
     * @param date
     * @param amount
     * @return
     */
    public static Date getChangeSecond(Date date, int amount) {
        return DateTimeUtil.getChangeCalendar(date, Calendar.SECOND, amount);
    }

    public static String getChangeSecondStr(Date date, int amount) {
        return DateTimeUtil.getChangeCalendar(date.getTime(), Calendar.SECOND, amount, DateTimeUtil.DEFAULT_DATE_PT);
    }

    /**
     * 주어진 날짜를 일, 시간, 분을 +, - 한다.
     *
     * @param date
     * @param field  , Calendar.DAY_OF_MONTH, Calendar.HOUR_OF_DAY, Calendar.MINUTE
     * @param amount
     * @return
     */
    public static Date getChangeCalendar(Date date, int field, int amount) {
        return DateTimeUtil.getChangeCalendar(date.getTime(), field, amount);
    }

    public static Date getChangeCalendar(long longDate, int field, int amount) {
        Calendar c = Calendar.getInstance();
        c.clear();

        c.setTimeInMillis(longDate);
        c.add(field, amount);
        return c.getTime();
    }

    public static String getChangeCalendar(Long mill, int field, int amount, String pattern) {
        Calendar c = Calendar.getInstance();
        c.clear();
        c.setTimeInMillis(mill);
        c.add(field, amount);
        return DateTimeUtil.getSimpleDateFormat(c, pattern);
    }

    /**
     * 현재 시간의 시분초를 0으로 초기화한 Date 객체 생성
     *
     * @return
     */
    public static Date getZeroTimeDate() {
        Calendar c = Calendar.getInstance();
        c.set(Calendar.HOUR, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.SECOND, 0);
        Date d = new Date(c.getTimeInMillis());
        return d;
    }

    public static Date getNowZeroTimeSec() {
        Calendar c = Calendar.getInstance();
        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.SECOND, 0);
        Date d = new Date(c.getTimeInMillis());
        return d;
    }

    /**
     * 분,초를 0 으로 초기화
     *
     * @return
     */
    public static Date getNowZeroTimeMinute() {
        Calendar c = Calendar.getInstance();
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.SECOND, 0);
        Date d = new Date(c.getTimeInMillis());
        return d;
    }

    public static Date getZeroTimeDate(Date date) {
        Calendar c = Calendar.getInstance();
        c.clear();
        c.setTimeInMillis(date.getTime());
        c.set(Calendar.HOUR, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.SECOND, 0);
        Date d = new Date(c.getTimeInMillis());
        return d;
    }

    public static Date getZeroTimeMinute(Date date) {
        Calendar c = Calendar.getInstance();
        c.clear();
        c.setTimeInMillis(date.getTime());
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.SECOND, 0);
        Date d = new Date(c.getTimeInMillis());
        return d;
    }

    public static Date getZeroTimeMinute(String date, String pattern) {
        Date eDate = DateTimeUtil.getStrToDateTime(date, pattern);
        Date zeroDate = DateTimeUtil.getZeroTimeMinute(eDate);
        return zeroDate;
    }

    /**
     * 날짜 시간을 00시00분00초로 변경한다. 20140101 -->20140101000000, 20140101000099 --> 20140101000000
     *
     * @param sdate 최소 8자리
     * @return
     * @throws Exception
     */
    public static String makeBeginTime(String sdate) throws Exception {
        String pattern = "yyyyMMdd";
        if (sdate.length() < pattern.length()) {
            throw new Exception("date format(yyyymmss) error , " + sdate);
        }
        String rtn = sdate.substring(0, pattern.length());
        return rtn + "000000";
    }

    /**
     * 날짜 시간을 23시59분59초로 변경한다. 20140101 -->20140101235959, 20140101000099 --> 20140101235959
     *
     * @param sdate 최소 8자리
     * @return
     * @throws Exception
     */
    public static String makeEndTime(String sdate) throws Exception {
        String pattern = "yyyyMMdd";
        if (sdate.length() < pattern.length()) {
            throw new Exception("date format(yyyymmss) error , " + sdate);
        }
        String rtn = sdate.substring(0, pattern.length());
        return rtn + "235959";
    }

    /**
     * 마지막 월, 일
     *
     * @param date
     * @param field
     * @return
     */
    public static int getLastDay(Date date, int field) {
        Calendar c = Calendar.getInstance();
        c.clear();
        c.setTimeInMillis(date.getTime());
        return c.getActualMaximum(field);
    }

    public static int getDayOfWeek(Date date) {
        Calendar c = Calendar.getInstance();
        c.clear();
        c.setTimeInMillis(date.getTime());
        return c.get(Calendar.DAY_OF_WEEK);
    }

    public static long diffOfDate(String begin, String end) throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        Date beginDate = formatter.parse(begin);
        Date endDate = formatter.parse(end);

        long diff = endDate.getTime() - beginDate.getTime();
        long diffDays = diff / (24 * 60 * 60 * 1000);
        return diffDays;
    }

    /**
     * 두 날짜의 특정 필드의 차이를 비교한다.
     *
     * @param field Calendar.HOUR_OF_DAY, Calendar.MINUTE, Calendar.SECOND
     * @param sDate
     * @param eDate
     * @return
     */
    public static long getDiffDate(int field, Date sDate, Date eDate) {
        long diff = eDate.getTime() - sDate.getTime();
        if (field == Calendar.HOUR_OF_DAY) {
            return (diff / (60 * 60 * 1000));
        } else if (field == Calendar.MINUTE) {
            return (diff / (60 * 1000));
        } else if (field == Calendar.SECOND) {
            return (diff / 1000);
        }
        return diff;
    }


}
