package bigdata.course.hw2.ipbytes;

import bigdata.course.hw2.ipbytes.customwritable.BytesInfo;
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


public class CustomMapperTest {

    private MapDriver<LongWritable, Text, IntWritable, BytesInfo> customMapDriver;

    @Before
    public void setUp() {
        IpBytesMapper.CustomMapper customMapper = new IpBytesMapper.CustomMapper();
        customMapDriver = MapDriver.newMapDriver(customMapper);
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

        List<Pair<IntWritable, BytesInfo>> listOut = new ArrayList<>();
        listOut.add(new Pair<>(new IntWritable(1), new BytesInfo(40028)));
        listOut.add(new Pair<>(new IntWritable(1), new BytesInfo(56928)));
        listOut.add(new Pair<>(new IntWritable(2), new BytesInfo(14917)));
        listOut.add(new Pair<>(new IntWritable(2), new BytesInfo(390)));

        customMapDriver.withAll(list);
        customMapDriver.withAllOutput(listOut);

        customMapDriver.runTest();
        assertEquals("Expected 1 counter increment", 3, customMapDriver.getCounters()
                .findCounter(IpBytesMapper.MapperCounter.INVALID_RECORD).getValue());
    }

    @Test
    public void mapTestEmptyString() throws IOException {

        customMapDriver.withInput(new LongWritable(5), new Text(""));
        customMapDriver.withInput(new LongWritable(5), new Text("  "));
        customMapDriver.withInput(new LongWritable(5), new Text("  \t"));

        customMapDriver.runTest();
        assertEquals("Expected 3 counter increment", 3, customMapDriver.getCounters()
                .findCounter(IpBytesMapper.MapperCounter.INVALID_RECORD).getValue());
    }

    @Test
    public void mapTestWithDash() throws IOException {

        customMapDriver.withInput(new LongWritable(1), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 - \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        customMapDriver.withInput(new LongWritable(2), new Text("ip1 - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 200 56928 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        customMapDriver.withInput(new LongWritable(3), new Text("ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ HTTP/1.1\" 200 - \"http://www.stumbleupon.com/refer.php?url=http%3A%2F%host1%2Fsun_ss5%2F\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\""));
        customMapDriver.withInput(new LongWritable(4), new Text("ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/pdf.gif HTTP/1.1\" 200 390 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\""));

        customMapDriver.withOutput(new IntWritable(1), new BytesInfo(0));
        customMapDriver.withOutput(new IntWritable(1), new BytesInfo(56928));
        customMapDriver.withOutput(new IntWritable(2), new BytesInfo(0));
        customMapDriver.withOutput(new IntWritable(2), new BytesInfo(390));

        customMapDriver.runTest();
    }

    @Test
    public void mapTestWithWrongIp() throws IOException {

        customMapDriver.withInput(new LongWritable(1), new Text("ipSS - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 - \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        customMapDriver.withInput(new LongWritable(2), new Text("ipTT - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 200 56928 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));

        customMapDriver.runTest();
        assertEquals("Expected 2 counter increment", 2, customMapDriver.getCounters()
                .findCounter(IpBytesMapper.MapperCounter.INVALID_RECORD).getValue());

    }

    @Test
    public void mapTestWithWrongBytes() throws IOException {

        customMapDriver.withInput(new LongWritable(1), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 MISTAKE \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        customMapDriver.withInput(new LongWritable(2), new Text("ip1 - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 200 !!!! \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));

        customMapDriver.runTest();
        assertEquals("Expected 2 counter increment", 2, customMapDriver.getCounters()
                .findCounter(IpBytesMapper.MapperCounter.INVALID_RECORD).getValue());
    }
}