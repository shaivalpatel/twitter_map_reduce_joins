package TwitterTriangleMR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class ReplicatedJoinDriver {
    private static final Logger logger = LogManager.getLogger(ReplicatedJoinDriver.class);
    public static enum Count{
        numtriangles;
    }


    public static class MaxFilterMapper extends Mapper<Object, Text,Text, NullWritable>{
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable tuple = new IntWritable();
        private Text word = new Text();
        private Text word1 = new Text();
        private int MAX = 50000;
        //map function which takes each line splits it by comma and takes second word in the temp list and emmits that word and 1 as tuple.
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {


            String[] temp =value.toString().split(",");// Splitting the line by comma into a list
            if( Integer.parseInt(temp[0])<=MAX && Integer.parseInt(temp[1])<=MAX){
                word.set(value);
                context.write(word,NullWritable.get());

            }


        }
    }
    public static class ReplicatedJoinMapper extends
            Mapper<Object, Text, Text, Text> {

        private HashMap<String, List<String>> userIdToInfo = new HashMap<String, List<String> >();

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            try {


                Configuration conf1 = context.getConfiguration();

                String p = conf1.get("data");
                FileSystem fs= FileSystem.get(URI.create(p),conf1);
                FileStatus[] files = fs.listStatus(new Path (p));

                if (files == null || files.length == 0) {
                    throw new RuntimeException(
                            "User information is not set in DistributedCache");
                }



                // Read all files in the DistributedCache
                for (FileStatus s : files) {
                    BufferedReader rdr = new BufferedReader(
                            new InputStreamReader(fs.open(s.getPath())));

                    String line;
                    // For each record in the user file
                    while ((line = rdr.readLine()) != null) {
                        List<String> list = new ArrayList<String>();
                        // Get the user ID for this record
                        String[] temp= line.split(",");
                        String follower = temp[0];
                        String followee = temp[1];
                        list.add(followee);

                        if (follower!=null){
                            if(!userIdToInfo.containsKey(follower)){
                                userIdToInfo.put(follower,list);

                            }
                            else{
                                List<String> tempList= userIdToInfo.get(follower);
                                    tempList.add(followee);
                                    userIdToInfo.put(follower, tempList);
                            }

                        }

                    }
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Parse the input string into a nice map


            String[] temp  = value.toString().split(",");
            if (userIdToInfo.containsKey(temp[1])){
                List<String> matches = userIdToInfo.get(temp[1]);
                for( String item : matches){
                    String tp = temp[0]+","+temp[1]+","+item;
                    if (userIdToInfo.containsKey(item) ){
                        List<String> matches1 = userIdToInfo.get(item);
                        for(String x: matches1){
                            if (temp[0].equals(x)){
                                context.getCounter(Count.numtriangles).increment(1);
                            }
                        }
                    }
                }
            }



            // If the user information is not null, then output
            /*for (String element : twopath){
                    String[] tmp = element.split(",");
                    String m = tmp[2];
                    if (userIdToInfo.containsKey(m)){
                        List<String> matches1 = userIdToInfo.get(m);
                        for(String x: matches1){
                            if (tmp[0].equals(x)){
                                context.getCounter(Count.numtriangles).increment(1);
                            }
                        }
                    }

            }*/
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();

        //error handling
        if (otherArgs.length < 3) {
            logger.error("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }


        //Creating a job and passing configuration to it
        Job job1 = Job.getInstance(conf1, "Twitter Follower Count");
        job1.setJarByClass(ReplicatedJoinDriver.class);//Assigning the jar class

        //job.setReducerClass(IntSumReducer.class);//calling reduce on the combiner output
        final Configuration jobConf1 = job1.getConfiguration(); //getting job configuration
        jobConf1.set("mapreduce.output.textoutputformat.separator", "\t");
        job1.setMapperClass(MaxFilterMapper.class); //calling map task on the job


        //writing outputs
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);




        //setting paths

        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));

        FileOutputFormat.setOutputPath(job1,
                new Path(otherArgs[1]));


        //job.setReducerClass(IntSumReducer.class);//calling reduce on the combiner output

        if (!job1.waitForCompletion(true)){
            System.exit(1);
        }

        Configuration conf = new Configuration();


        if (otherArgs.length <3) {
            logger.error("Usage: ReplicatedJoin <user data> <comment data> <out> [inner|leftouter]");
            System.exit(1);
        }
        conf.set("data",new Path(otherArgs[1]).toString());


        // Configure the join type
        Job job =  Job.getInstance(conf, "Replicated Join");
        job.setJarByClass(ReplicatedJoinDriver.class);
        job.setMapperClass(ReplicatedJoinMapper.class);
        TextInputFormat.setInputPaths(job, new Path(otherArgs[1]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        logger.error(job.waitForCompletion(true) ? 0 : 1);
        Counter c = job.getCounters().findCounter(Count.numtriangles);
        long final_count = c.getValue();

        final_count/=3;
        logger.warn(final_count);


    }
}