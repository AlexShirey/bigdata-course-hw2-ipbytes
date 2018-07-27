package bigdata.course.hw2.ipbytes.customwritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Objects;

/**
 * The Custom Writable class that can be used to count total bytes (sum) and the average byte value (per this object)
 */
public class BytesInfo implements Writable {

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat(".##");
    private int bytes;
    private int count;

    public BytesInfo() {
    }

    //used in tests only
    public BytesInfo(int bytes) {
        this.bytes = bytes;
        count = 1;
    }

    //used in tests only
    public BytesInfo(int bytes, int count) {
        this.bytes = bytes;
        this.count = count;
    }

    public void add(BytesInfo bytesInfo) {
        bytes += bytesInfo.getBytes();
        count += bytesInfo.getCount();
    }

    public void clear() {
        bytes = 0;
        count = 0;
    }


    public void setBytes(int value) {
        bytes = value;
        count = 1;
    }

    public int getBytes() {
        return bytes;
    }

    public int getCount() {
        return count;
    }

    public double getAverage() {
        if (count != 0) {
            return (double) bytes / (double) count;
        }
        return 0;
    }

    /**
     * The implementation of the Raw Comparator.
     * Compares only by city id.
     */
    public static class Comparator extends WritableComparator {
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public Comparator() {
            super(CompositeCity.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            int cityId1 = readInt(b1, s1);
            int cityId2 = readInt(b2, s2);

            return Integer.compare(cityId1, cityId2);
        }
    }

    /**
     * Serialize the fields of this object to <code>out</code>.
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(bytes);
        out.writeInt(count);
    }

    /**
     * Deserialize the fields of this object from <code>in</code>.
     *
     * <p>For efficiency, implementations should attempt to re-use storage in the
     * existing object where possible.</p>
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        bytes = in.readInt();
        count = in.readInt();
    }

    @Override
    public String toString() {
        return bytes + "," + DECIMAL_FORMAT.format(getAverage());
    }


    //used in tests only
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BytesInfo bytesInfo = (BytesInfo) o;
        return bytes == bytesInfo.bytes &&
                count == bytesInfo.count;
    }

    //used in tests only
    @Override
    public int hashCode() {

        return Objects.hash(bytes, count);
    }
}
