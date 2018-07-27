package bigdata.course.hw2.ipbytes;

import bigdata.course.hw2.ipbytes.customwritable.BytesInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class CustomReducerTest {

    private ReduceDriver<IntWritable, BytesInfo, IntWritable, BytesInfo> customReduceDriver;

    @Before
    public void setUp() {
        IpBytesCustomReducer reducer = new IpBytesCustomReducer();
        customReduceDriver = ReduceDriver.newReduceDriver(reducer);

    }

    @Test
    public void reduceTest() throws IOException {

        customReduceDriver.withInput(new IntWritable(1), Arrays.asList(new BytesInfo(10), new BytesInfo(20)));
        customReduceDriver.withInput(new IntWritable(2), Arrays.asList(new BytesInfo(5), new BytesInfo(5)));
        customReduceDriver.withInput(new IntWritable(3), Arrays.asList(new BytesInfo(0), new BytesInfo(10)));

        customReduceDriver.withOutput(new IntWritable(1), new BytesInfo(30, 2));
        customReduceDriver.withOutput(new IntWritable(2), new BytesInfo(10, 2));
        customReduceDriver.withOutput(new IntWritable(3), new BytesInfo(10, 2));

        customReduceDriver.runTest();

    }

    @Test
    public void reduceTestNoData() throws IOException {

        customReduceDriver.withInput(new IntWritable(1), Arrays.asList(new BytesInfo(0), new BytesInfo(0)));
        customReduceDriver.runTest();

    }
}