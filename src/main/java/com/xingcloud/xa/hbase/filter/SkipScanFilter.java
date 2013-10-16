package com.xingcloud.xa.hbase.filter;

import com.xingcloud.xa.hbase.model.KeyRange;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-9-15
 * Time: 下午8:46
 * To change this template use File | Settings | File Templates.
 */
public class SkipScanFilter extends FilterBase {
  private static Log LOG = LogFactory.getLog(SkipScanFilter.class);

  private List<KeyRange> slot;
  private boolean isDone;
  private int position;

  private Pair<byte[], byte[]> uidRange;

  private SkipType skipType;

  private static byte[] maxUid = {-1,-1,-1,-1,-1};

  public SkipScanFilter() {
    super();
  }

  public SkipScanFilter(List<KeyRange> slot) {
    init(slot, null);
  }

  public SkipScanFilter(List<KeyRange> slot, Pair<byte[], byte[]> uidRange) {
    init(slot, uidRange);
  }

  private void init(List<KeyRange> slot, Pair<byte[], byte[]> uidRange) {
    if (slot.isEmpty()) {
      throw new IllegalStateException("Key range slot is empty!");
    }
    this.slot = slot;
    this.uidRange = uidRange;
    this.position = 0;
  }

  @Override
  public boolean filterAllRemaining() {
    return isDone;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue kv) {
    return navigate(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength());
  }

  @Override
  public KeyValue getNextKeyHint(KeyValue kv) {
    byte[] skipKV = null;
    switch (skipType) {
      case SkipToUidStart:
        byte[] prefix = Arrays.copyOfRange(kv.getRow(), 0, kv.getRow().length-5);
        skipKV = Bytes.add(prefix, uidRange.getFirst());
        break;
      case SkipToUidEnd:
        prefix = Arrays.copyOfRange(kv.getRow(), 0, kv.getRow().length-5);
        skipKV = Bytes.add(prefix, maxUid);
        break;
      default:
        skipKV = slot.get(position).getLowerRange();
    }
    return isDone ? null : new KeyValue(skipKV, kv.getFamily(), kv.getQualifier());

  }

  private ReturnCode navigate(final byte[] currentKey, final int offset, final int length) {
    while (position < slot.size()) {
      if (slot.get(position).compareUpperToLowerBound(currentKey, offset, length) < 0) {
        position++;
      } else {
        break;
      }
    }
    if (position >= slot.size()) {
      isDone = true;
      return ReturnCode.NEXT_ROW;
    }

    if (slot.get(position).compareLowerToUpperBound(currentKey, offset, length) <= 0) {
      //当前kv属于此key range
      if (uidRange != null) {
        //判断是否在指定的uid范围内
        if (Bytes.compareTo(currentKey, offset+length-5, length, uidRange.getFirst(), 0, uidRange.getFirst().length) < 0) {
          //如果当前uid小于指定的起始uid，则跳到当前事件的指定起始uid位置
          skipType = SkipType.SkipToUidStart;
          return ReturnCode.SEEK_NEXT_USING_HINT;
        } else if (Bytes.compareTo(currentKey, offset-5, length, uidRange.getSecond(), 0, uidRange.getSecond().length) > 0) {
          //如果当前uid大于指定的结束uid，则跳到当前事件可能的最大uid位置（实际上会跳到下一事件的起始uid位置）
          skipType = SkipType.SkipToUidEnd;
          return ReturnCode.SEEK_NEXT_USING_HINT;
        }
      }
      return ReturnCode.INCLUDE;
    } else {
      skipType = SkipType.Normal;
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(slot.size());
    for (KeyRange range : slot) {
      range.write(out);
    }
    if (uidRange != null) {
      out.writeBoolean(true);
      out.writeLong(Bytes.toLong(uidRange.getFirst()));
      out.writeLong(Bytes.toLong(uidRange.getSecond()));
    } else {
      out.writeBoolean(false);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int slotSize = in.readInt();
    slot = new ArrayList<KeyRange>(slotSize);
    for (int i=0; i<slotSize; i++) {
      KeyRange range = new KeyRange();
      range.readFields(in);
      slot.add(range);
    }
    boolean hasUidRange = in.readBoolean();
    if (hasUidRange) {
      long suid = in.readLong();
      long euid = in.readLong();
      this.uidRange = new Pair<byte[], byte[]>(Bytes.toBytes(suid), Bytes.toBytes(euid));
    }
  }

  public enum SkipType{
    SkipToUidStart, SkipToUidEnd, Normal
  }
}
