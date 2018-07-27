package bigdata.course.hw2.ipbytes;

import bigdata.course.hw2.ipbytes.customwritable.BytesInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * The Reducer class that uses custom writable (BytesInfo).
 * This reducer is also used as Combiner.
 */
public class IpBytesCustomReducer extends Reducer<IntWritable, BytesInfo, IntWritable, BytesInfo> {

    //custom writable
    private BytesInfo bytesInfo = new BytesInfo();

    @Override
    protected void reduce(IntWritable key, Iterable<BytesInfo> values, Context context) throws IOException, InterruptedException {

        //add all values to the one object
        for (BytesInfo value : values) {
            bytesInfo.add(value);
        }

        //In case if some reducer doesn't get any information its output should be omitted
        if (bytesInfo.getBytes() != 0) {
            context.write(key, bytesInfo);
        }

        //to clear all values in the object to reuse it for a next key
        bytesInfo.clear();
    }

}
