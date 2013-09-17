package com.xingcloud.xa.hbase.util;

import com.xingcloud.xa.hbase.filter.SkipScanFilter;
import com.xingcloud.xa.hbase.model.KeyRange;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-9-17
 * Time: 上午10:07
 * To change this template use File | Settings | File Templates.
 */
public class EventTableUtil {
  private static final Log LOG = LogFactory.getLog(EventTableUtil.class);

  public static String getEventFromDEURowKey(byte[] rowKey) {
    byte[] eventBytes = Arrays.copyOfRange(rowKey, 8, rowKey.length - 6);
    return Bytes.toString(eventBytes);
  }

  public static byte[] getUidOf5BytesFromDEURowKey(byte[] rowKey) {
    byte[] uid = Arrays.copyOfRange(rowKey, rowKey.length - 5, rowKey.length);
    return uid;
  }

  public static byte[] getRowKey(byte[] date, String event, byte[] uid) {

    byte[] rk = new byte[14 + event.length()];
    int index = 0;

    for (int i = 0; i < date.length; i++) {
      rk[index++] = date[i];
    }
    for (int i = 0; i < event.length(); i++) {
      rk[index++] = (byte) (event.charAt(i) & 0xFF);
    }

    rk[index++] = (byte) 0xff;

    for (int i = 0; i < uid.length; i++) {
      rk[index++] = uid[i];
    }

    return rk;
  }

  public static byte[] changeRowKeyByUid(byte[] rk, byte[] newUid) {
    byte[] nrk = Arrays.copyOf(rk, rk.length);

    int j = 0;
    for (int i = nrk.length - 5; i < nrk.length; i++) {
      nrk[i] = newUid[j++];
    }
    return nrk;
  }



  public static Pair<byte[], byte[]> getStartEndRowKey(String startDate, String endDate, List<String> sortedEvents, long startBucket, long offsetBucket) throws UnsupportedEncodingException {
    long startUid = startBucket << 32;
    long endBucket = offsetBucket + startBucket;
    long endUid = 0l;
    if (endBucket >= 256) {
      endUid = (1l << 40) - 1l;
    } else {
      endUid = endBucket << 32;
    }

    byte[] realSK = getRowKey(startDate.replace("-", ""), sortedEvents.get(0), startUid);
    byte[] realEK = getRowKey(endDate.replace("-", ""), sortedEvents.get(sortedEvents.size() - 1), endUid);
    return new Pair<byte[], byte[]>(realSK, realEK);
  }

  public static Pair<Long, Long> getStartEndUidPair() {
    long startUid = 0l << 32;
    long endUid = (1l << 40) - 1l;

    return new Pair<Long, Long>(startUid, endUid);
  }

  public static Pair<Long, Long> getStartEndUidPair(long startBucket, long offsetBucket) {
    long startUid = startBucket << 32;
    long endBucket = offsetBucket + startBucket;
    long endUid = 0l;
    if (endBucket >= 256) {
      endUid = (1l << 40) - 1l;
    } else {
      endUid = endBucket << 32;
    }

    return new Pair<Long, Long>(startUid, endUid);
  }

  /**
   * 按照hbase里面event的排序来排这个eventlist ,加上0xff进行字典排序。
   */
  public static List<String> sortEventList(List<String> eventList) {

    List<String> events = new ArrayList<String>();
    for (String event : eventList)
      events.add(event + String.valueOf((char) 255));
    Collections.sort(events);

    List<String> results = new ArrayList<String>();
    for (String charEvent : events)
      results.add(charEvent.substring(0, charEvent.length() - 1));

    return results;
  }

  public static long getUidOfLongFromDEURowKey(byte[] rowKey) {
    byte[] uid = new byte[8];
    int i = 0;
    for (; i < 3; i++) {
      uid[i] = 0;
    }

    for (int j = rowKey.length - 5; j < rowKey.length; j++) {
      uid[i++] = rowKey[j];
    }

    return Bytes.toLong(uid);
  }

  public static Filter getSkipScanFilter(List<String> dates, List<String> events, Pair<Long, Long> uidRange) throws UnsupportedEncodingException {
    List<KeyRange> slot = new ArrayList<KeyRange>();
    for (String date : dates) {
      for (String event : events) {
        byte[] lowerRange = getRowKey(date, event, uidRange.getFirst());
        byte[] upperRange = getRowKey(date, event, uidRange.getSecond());
        KeyRange range = new KeyRange(lowerRange, true, upperRange, false);
        slot.add(range);
        System.out.println("Key range: " + range);
      }
    }
    return new SkipScanFilter(slot);
  }

  public static byte[] getRowKey(String date, String event, long suid) throws UnsupportedEncodingException {
    String tmpStr = date + event;
    byte[] bytesTmp = tmpStr.getBytes("UTF-8");
    int lenTmp = bytesTmp.length;

    byte[] rk = new byte[lenTmp+6];
    System.arraycopy(bytesTmp, 0, rk, 0, lenTmp);

    rk[lenTmp] = (byte) (0xff);
    rk[lenTmp+1] = (byte) (suid>>32 & 0xff);
    rk[lenTmp+2] = (byte) (suid>>24 & 0xff);
    rk[lenTmp+3] = (byte) (suid>>16 & 0xff);
    rk[lenTmp+4] = (byte) (suid>>8 & 0xff);
    rk[lenTmp+5] = (byte) (suid & 0xff);

    return rk;
  }

}
