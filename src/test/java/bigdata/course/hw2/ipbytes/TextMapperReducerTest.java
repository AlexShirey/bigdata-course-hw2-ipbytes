package bigdata.course.hw2.ipbytes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TextMapperReducerTest {

    private MapReduceDriver<LongWritable, Text, IntWritable, IntWritable, IntWritable, Text> mapReduceDriver;

    @Before
    public void setUp() {

        IpBytesMapper.TextMapper mapper = new IpBytesMapper.TextMapper();
        IpBytesTextReducer reducer = new IpBytesTextReducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

    }

    @Test
    public void mapReduceTest() throws IOException {

        mapReduceDriver.withInput(new LongWritable(1), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 10 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        mapReduceDriver.withInput(new LongWritable(2), new Text("ip1 - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 200 10 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        mapReduceDriver.withInput(new LongWritable(3), new Text("ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ HTTP/1.1\" 200 15 \"http://www.stumbleupon.com/refer.php?url=http%3A%2F%host1%2Fsun_ss5%2F\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\""));
        mapReduceDriver.withInput(new LongWritable(4), new Text("ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/pdf.gif HTTP/1.1\" 200 40 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\""));

        mapReduceDriver.withOutput(new IntWritable(1), new Text("20,10.0"));
        mapReduceDriver.withOutput(new IntWritable(2), new Text("55,27.5"));

        mapReduceDriver.run();
    }

}
