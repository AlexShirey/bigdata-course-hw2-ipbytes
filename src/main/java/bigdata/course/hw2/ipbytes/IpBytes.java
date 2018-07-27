package bigdata.course.hw2.ipbytes;

import bigdata.course.hw2.ipbytes.customwritable.BytesInfo;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * The Driver for the IpBytes app, supports handling of generic command-line options.
 */
public class IpBytes extends Configured implements Tool {

    private final static Logger LOGGER = Logger.getLogger(IpBytes.class);

    /**
     * Main method, invokes method run.
     *
     * @param args - command specific arguments.
     * @see #run(String[] args)
     */
    public static void main(String[] args) throws Exception {

        LOGGER.info("starting the app... \n");
        int result = ToolRunner.run(new Configuration(), new IpBytes(), args);
        LOGGER.info("...the execution of the app is finished! \n");

        System.exit(result);
    }

    /**
     * This method parses the args,
     * sets the configuration of the job and
     * submits the job to the cluster for execution.
     * <p>
     * The app support option [-seqSnappy],
     * in this case the custom type is used (in IpBytesMapper and Reducer), the output is a sequence file compressed by Snappy codec.
     * Otherwise, default Text type is used, output is a text file (CSV) without compression.
     * So, there are two job configurations which depend on args passed in a command line.
     *
     * @param args - command specific arguments.
     * @return - result of the execution, 0 - if the job succeeded
     */
    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "HW2 - Ip Bytes");
        job.setJarByClass(getClass());

        if (args.length == 2) {
            setJobForText(job, conf);
        } else if (args.length == 3 && args[2].equals("-seqSnappy")) {
            System.out.println("Output will be a sequence file, compressed by Snappy codec. Starting the job...");
            setJobForSeqSnappy(job);
        } else {
            System.err.print("Usage: IpBytes <input path> <output path> [-seqSnappy] \n");
            return -1;
        }

        String hdfsInputPath = args[0];
        String hdfsOutputPath = args[1];

        FileInputFormat.addInputPath(job, new Path(hdfsInputPath));
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutputPath));

        boolean jobComplete = job.waitForCompletion(true);

        if (jobComplete) {
            System.out.println("Job is finished, printing the number of users by browser");
            String counterGroupName = "User browser";
            printCounterGroup(job, counterGroupName);
            return 0;
        }

        return 1;
    }

    /**
     * Sets the configuration for default job (Text type, CSV, no compression)
     *
     * @param job  - job to set configuration
     * @param conf - configuration to set special properties
     */
    private void setJobForText(Job job, Configuration conf) {

        job.setMapperClass(IpBytesMapper.TextMapper.class);
        job.setReducerClass(IpBytesTextReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        conf.set("mapreduce.output.textoutputformat.separator", ",");
    }

    /**
     * Sets the configuration for SeqSnappy job (Custom type, sequence file, Snappy compression)
     *
     * @param job - job to set configuration
     */
    private void setJobForSeqSnappy(Job job) {

        job.setMapperClass(IpBytesMapper.CustomMapper.class);
        job.setCombinerClass(IpBytesCustomReducer.class);
        job.setReducerClass(IpBytesCustomReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BytesInfo.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BytesInfo.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

    }

    /**
     * Prints all counters (name and value) in the counter group
     *
     * @param job              - job to get counters
     * @param counterGroupName - name of the counter group
     */
    private void printCounterGroup(Job job, String counterGroupName) throws IOException {

        Counters counters = job.getCounters();
        CounterGroup countersGroup = counters.getGroup(counterGroupName);

        for (Counter counter : countersGroup) {
            System.out.println(counter.getName() + " " + counter.getValue());
        }
    }

}
