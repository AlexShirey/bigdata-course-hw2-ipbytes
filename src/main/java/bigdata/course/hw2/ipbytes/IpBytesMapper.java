package bigdata.course.hw2.ipbytes;

import bigdata.course.hw2.ipbytes.customwritable.BytesInfo;
import bigdata.course.hw2.ipbytes.util.RecordParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This class contains mapper enum counter and two static Mappers for different job config
 */
public class IpBytesMapper {

    private final static String USER_BROWSER_GROUP = "User browser";

    enum MapperCounter {
        INVALID_RECORD
    }

    /**
     * The default Mapper class that doesn't use custom Writable
     */
    public static class TextMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private IntWritable ip = new IntWritable();
        private IntWritable bytes = new IntWritable();
        private RecordParser parser = new RecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //check the record for validity
            if (!parser.parseRecord(value.toString())) {
                context.getCounter(MapperCounter.INVALID_RECORD).increment(1);
                return;
            }

            //dynamic counter, counts users per browser
            context.getCounter(IpBytesMapper.USER_BROWSER_GROUP, parser.getBrowser()).increment(1);

            ip.set(parser.getIp());
            bytes.set(parser.getBytes());

            context.write(ip, bytes);
        }
    }

    /**
     * The Custom Mapper class that uses custom Writable (BytesInfo)
     */
    public static class CustomMapper extends Mapper<LongWritable, Text, IntWritable, BytesInfo> {

        private IntWritable ip = new IntWritable();
        private BytesInfo bytesInfo = new BytesInfo();
        private RecordParser parser = new RecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //check the record for validity
            if (!parser.parseRecord(value.toString())) {
                context.getCounter(MapperCounter.INVALID_RECORD).increment(1);
                return;
            }

            //dynamic counter, counts users per browser
            context.getCounter(IpBytesMapper.USER_BROWSER_GROUP, parser.getBrowser()).increment(1);

            ip.set(parser.getIp());
            bytesInfo.setBytes(parser.getBytes());

            context.write(ip, bytesInfo);
        }
    }

}
