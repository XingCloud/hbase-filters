package com.xingcloud.xa.hbase.model;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-9-16
 * Time: 上午12:21
 * To change this template use File | Settings | File Templates.
 */
public class KeyRange implements Writable {

  private static final byte[] DEGENERATE_KEY = new byte[] {1};

  private byte[] lowerRange;
  private boolean lowerInclusive;
  private byte[] upperRange;
  private boolean upperInclusive;
  private boolean isSingleKey;

  public KeyRange() {
    this.lowerRange = DEGENERATE_KEY;
    this.lowerInclusive = false;
    this.upperRange = DEGENERATE_KEY;
    this.upperInclusive = false;
    this.isSingleKey = false;
  }

  public KeyRange(byte[] lowerRange, boolean lowerInclusive, byte[] upperRange, boolean upperInclusive) {
    this.lowerRange = lowerRange;
    this.lowerInclusive = lowerInclusive;
    this.upperRange = upperRange;
    this.upperInclusive = upperInclusive;
    init();
  }

  private void init() {
    this.isSingleKey = lowerInclusive && upperInclusive && Bytes.compareTo(lowerRange, upperRange) == 0;
  }

  public int compareLowerToUpperBound(byte[] b, int o, int l) {
    return compareLowerToUpperBound(b, o, l, true);
  }

  public int compareLowerToUpperBound(byte[] b, int o, int l, boolean isInclusive) {
    int cmp = Bytes.compareTo(lowerRange, 0, lowerRange.length, b, o, l);
    if (cmp > 0) {
      return 1;
    }
    if (cmp < 0) {
      return -1;
    }
    if (lowerInclusive && isInclusive) {
      return 0;
    }
    return 1;
  }

  public int compareUpperToLowerBound(byte[] b, int o, int l) {
    return compareUpperToLowerBound(b, o, l, true);
  }

  public int compareUpperToLowerBound(byte[] b, int o, int l, boolean isInclusive) {
    int cmp = Bytes.compareTo(upperRange, 0, upperRange.length, b, o, l);
    if (cmp > 0) {
      return 1;
    }
    if (cmp < 0) {
      return -1;
    }
    if (upperInclusive && isInclusive) {
      return 0;
    }
    return -1;
  }


  public byte[] getLowerRange() {
    return lowerRange;
  }

  public void setLowerRange(byte[] lowerRange) {
    this.lowerRange = lowerRange;
  }

  public byte[] getUpperRange() {
    return upperRange;
  }

  public void setUpperRange(byte[] upperRange) {
    this.upperRange = upperRange;
  }

  public boolean isSingleKey() {
    return isSingleKey;
  }

  public boolean accept(byte[] b, int o, int l) {
    return compareUpperToLowerBound(b, o, l) >= 0 && compareLowerToUpperBound(b, o, l) <= 0;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(lowerRange);
    if (lowerRange != null)
      result = prime * result + (lowerInclusive ? 1231 : 1237);
    result = prime * result + Arrays.hashCode(upperRange);
    if (upperRange != null)
      result = prime * result + (upperInclusive ? 1231 : 1237);
    return result;
  }

  @Override
  public String toString() {
    if (isSingleKey()) {
      return Bytes.toStringBinary(lowerRange);
    }
    return (lowerInclusive ? "[" :
            "(") + (
            Bytes.toStringBinary(lowerRange)) + " - " + (
            Bytes.toStringBinary(upperRange)) + (upperInclusive ? "]" : ")" );
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof KeyRange)) {
      return false;
    }
    KeyRange that = (KeyRange)o;
    return Bytes.compareTo(this.lowerRange,that.lowerRange) == 0 && this.lowerInclusive == that.lowerInclusive &&
            Bytes.compareTo(this.upperRange, that.upperRange) == 0 && this.upperInclusive == that.upperInclusive;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, lowerRange.length);
    out.write(lowerRange);
    WritableUtils.writeVInt(out, upperRange.length);
    out.write(upperRange);
    out.writeBoolean(lowerInclusive);
    out.writeBoolean(upperInclusive);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int lowerSize = WritableUtils.readVInt(in);
    lowerRange = new byte[lowerSize];
    in.readFully(lowerRange);
    int upperSize = WritableUtils.readVInt(in);
    upperRange = new byte[upperSize];
    in.readFully(upperRange);
    lowerInclusive = in.readBoolean();
    upperInclusive = in.readBoolean();
  }
}
