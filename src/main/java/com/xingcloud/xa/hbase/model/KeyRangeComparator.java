package com.xingcloud.xa.hbase.model;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Comparator;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/4/14
 * Time: 11:20 AM
 * To change this template use File | Settings | File Templates.
 */
public class KeyRangeComparator implements Comparator<KeyRange> {
  @Override
  public int compare(KeyRange o1, KeyRange o2) {
    KeyRange range1 = (KeyRange) o1;
    byte[] lowerRange1 = range1.getLowerRange();
    KeyRange range2 = (KeyRange) o2;
    byte[] lowerRange2 = range2.getLowerRange();
    return Bytes.compareTo(lowerRange1, lowerRange2);
  }

  @Override
  public boolean equals(Object obj) {
    return false;
  }
}
