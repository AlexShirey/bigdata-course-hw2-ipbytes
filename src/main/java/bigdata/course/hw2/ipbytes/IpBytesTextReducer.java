package bigdata.course.hw2.ipbytes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * The default Reducer class that uses Text class for output.
 */
public class IpBytesTextReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat(".##");
    private Text outValue = new Text();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        int count = 0;

        for (IntWritable bytes : values) {
            sum += bytes.get();
            count++;
        }

        //In case if some reducer doesn't get any information its output should be omitted
        if (sum != 0) {
            outValue.set(String.valueOf(sum) + "," + getFormattedAverage(sum, count));
            context.write(key, outValue);
        }
    }

    /**
     * Counts the average and formats the value
     *
     * @param sum   - sum
     * @param count - count
     * @return - formatted string that represents the average value
     */
    private String getFormattedAverage(int sum, int count) {
        double average = (double) sum / (double) count;
        return DECIMAL_FORMAT.format(average);
    }
}
