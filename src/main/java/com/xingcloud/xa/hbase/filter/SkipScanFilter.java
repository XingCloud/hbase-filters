package com.xingcloud.xa.hbase.filter;

import com.xingcloud.xa.hbase.model.KeyRange;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
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

  public SkipScanFilter() {
    super();
  }

  public SkipScanFilter(List<KeyRange> slot) {
    init(slot);
  }

  private void init(List<KeyRange> slot) {
    if (slot.isEmpty()) {
      throw new IllegalStateException("Key range slot is empty!");
    }
    this.slot = slot;
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
    return isDone ? null : new KeyValue(slot.get(position).getLowerRange(), kv.getFamily(), kv.getQualifier());
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
    if (slot.get(position).compareLowerToUpperBound(currentKey, offset, length) <= 0) { //当前kv属于此key range
      return ReturnCode.INCLUDE;
    } else {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(slot.size());
    for (KeyRange range : slot) {
      range.write(out);
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
  }

  public static void main(String[] args) {
    int[] uids = {237, 39, 103, 263};
    for (int i=0; i<uids.length; i++) {
      long uid = UidMappingUtil.getInstance().decorateWithMD5(uids[i]);
      System.out.println(Bytes.toStringBinary(Bytes.toBytes(uid)));
    }

  }
}
