package bigdata.course.hw2.ipbytes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class TextMapperTest {

    private MapDriver<LongWritable, Text, IntWritable, IntWritable> textMapDriver;

    @Before
    public void setUp() {
        IpBytesMapper.TextMapper textMapper = new IpBytesMapper.TextMapper();
        textMapDriver = MapDriver.newMapDriver(textMapper);

    }

    @Test
    public void mapTest() throws IOException {

        List<Pair<LongWritable, Text>> list = new ArrayList<>();
        list.add(new Pair<>(new LongWritable(1), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"")));
        list.add(new Pair<>(new LongWritable(2), new Text("ip1 - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 200 56928 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"")));
        list.add(new Pair<>(new LongWritable(3), new Text("ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ HTTP/1.1\" 200 14917 \"http://www.stumbleupon.com/refer.php?url=http%3A%2F%host1%2Fsun_ss5%2F\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"")));
        list.add(new Pair<>(new LongWritable(4), new Text("ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/pdf.gif HTTP/1.1\" 200 390 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"")));
        list.add(new Pair<>(new LongWritable(5), new Text("")));
        list.add(new Pair<>(new LongWritable(5), new Text("  ")));
        list.add(new Pair<>(new LongWritable(5), new Text("  \t")));

        List<Pair<IntWritable, IntWritable>> listOut = new ArrayList<>();
        listOut.add(new Pair<>(new IntWritable(1), new IntWritable(40028)));
        listOut.add(new Pair<>(new IntWritable(1), new IntWritable(56928)));
        listOut.add(new Pair<>(new IntWritable(2), new IntWritable(14917)));
        listOut.add(new Pair<>(new IntWritable(2), new IntWritable(390)));

        textMapDriver.withAll(list);
        textMapDriver.withAllOutput(listOut);

        textMapDriver.runTest();
        assertEquals("Expected 1 counter increment", 3, textMapDriver.getCounters()
                .findCounter(IpBytesMapper.MapperCounter.INVALID_RECORD).getValue());
    }

    @Test
    public void mapTestEmptyString() throws IOException {

        textMapDriver.withInput(new LongWritable(5), new Text(""));
        textMapDriver.withInput(new LongWritable(5), new Text("  "));
        textMapDriver.withInput(new LongWritable(5), new Text("  \t"));

        textMapDriver.runTest();
        assertEquals("Expected 3 counter increment", 3, textMapDriver.getCounters()
                .findCounter(IpBytesMapper.MapperCounter.INVALID_RECORD).getValue());
    }

    @Test
    public void mapTestWithDash() throws IOException {

        textMapDriver.withInput(new LongWritable(1), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 - \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        textMapDriver.withInput(new LongWritable(2), new Text("ip1 - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 200 56928 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        textMapDriver.withInput(new LongWritable(3), new Text("ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ HTTP/1.1\" 200 - \"http://www.stumbleupon.com/refer.php?url=http%3A%2F%host1%2Fsun_ss5%2F\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\""));
        textMapDriver.withInput(new LongWritable(4), new Text("ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/pdf.gif HTTP/1.1\" 200 390 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\""));

        textMapDriver.withOutput(new IntWritable(1), new IntWritable(0));
        textMapDriver.withOutput(new IntWritable(1), new IntWritable(56928));
        textMapDriver.withOutput(new IntWritable(2), new IntWritable(0));
        textMapDriver.withOutput(new IntWritable(2), new IntWritable(390));

        textMapDriver.runTest();
    }

    @Test
    public void mapTestWithWrongIp() throws IOException {

        textMapDriver.withInput(new LongWritable(1), new Text("ipSS - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 - \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        textMapDriver.withInput(new LongWritable(2), new Text("ipTT - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 200 56928 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));

        textMapDriver.runTest();
        assertEquals("Expected 2 counter increment", 2, textMapDriver.getCounters()
                .findCounter(IpBytesMapper.MapperCounter.INVALID_RECORD).getValue());

    }

    @Test
    public void mapTestWithWrongBytes() throws IOException {

        textMapDriver.withInput(new LongWritable(1), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 MISTAKE \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        textMapDriver.withInput(new LongWritable(2), new Text("ip1 - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 200 !!!! \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));

        textMapDriver.runTest();
        assertEquals("Expected 2 counter increment", 2, textMapDriver.getCounters()
                .findCounter(IpBytesMapper.MapperCounter.INVALID_RECORD).getValue());
    }
}