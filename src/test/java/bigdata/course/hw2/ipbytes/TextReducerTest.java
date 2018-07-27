package bigdata.course.hw2.ipbytes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class TextReducerTest {

    private ReduceDriver<IntWritable, IntWritable, IntWritable, Text> reduceDriver;

    @Before
    public void setUp() {
        IpBytesTextReducer reducer = new IpBytesTextReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

    }

    @Test
    public void reduceTest() throws IOException {

        reduceDriver.withInput(new IntWritable(1), Arrays.asList(new IntWritable(10), new IntWritable(20)));
        reduceDriver.withInput(new IntWritable(2), Arrays.asList(new IntWritable(5), new IntWritable(5)));
        reduceDriver.withInput(new IntWritable(3), Arrays.asList(new IntWritable(0), new IntWritable(10)));

        reduceDriver.withOutput(new IntWritable(1), new Text("30,15.0"));
        reduceDriver.withOutput(new IntWritable(2), new Text("10,5.0"));
        reduceDriver.withOutput(new IntWritable(3), new Text("10,5.0"));

        reduceDriver.runTest();

    }

    @Test
    public void reduceTestNoData() throws IOException {

        reduceDriver.withInput(new IntWritable(1), Arrays.asList(new IntWritable(0), new IntWritable(0)));

        reduceDriver.runTest();

    }
}