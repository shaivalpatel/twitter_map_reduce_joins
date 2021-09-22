package TwitterTriangleMR;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MaxFilter{
    private static final Logger logger = LogManager.getLogger(MaxFilter.class);
    public static enum Count{
        numtriangles;
    }


    public static class MaxFilterMapper extends Mapper<Object, Text, Text,NullWritable>{
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable tuple = new IntWritable();
        private Text word = new Text();
        private Text word1 = new Text();

        //map function which takes each line splits it by comma and takes second word in the temp list and emmits that word and 1 as tuple.
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {


            String[] temp =value.toString().split(",");// Splitting the line by comma into a list
            if( Integer.parseInt(temp[0])<=1000 && Integer.parseInt(temp[1])<=1000){
                word.set(value);
                context.write(word,NullWritable.get());

            }
            //word.set(temp[1]);// setting word as the second word in the list
            //context.write((temp[0],temp[1]), one);// emmiting the second value in the list with as a tuple

        }
    }

    //Reduce class which has a reduce function called for each reduce call

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        //error handling
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }


        //Creating a job and passing configuration to it
        Job job = Job.getInstance(conf, "Twitter Follower Count");
        job.setJarByClass(MaxFilter.class);//Assigning the jar class
        final Configuration jobConf = job.getConfiguration(); //getting job configuration
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
        job.setMapperClass(MaxFilterMapper.class); //calling map task on the job

        //writing outputs
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //setting paths
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    }
