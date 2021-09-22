package TwitterTriangleMR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;



import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class RSJoin {
    private static final Logger logger = LogManager.getLogger(RSJoin.class);
    public static enum Count{
        numtriangles;
    }

    public static class MaxFilterMapper extends Mapper<Object, Text,Text,NullWritable>{
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable tuple = new IntWritable();
        private Text word = new Text();
        private Text word1 = new Text();

        //map function which takes each line splits it by comma and takes second word in the temp list and emmits that word and 1 as tuple.
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {


            String[] temp =value.toString().split(",");// Splitting the line by comma into a list
            if( Integer.parseInt(temp[0])<=5000 && Integer.parseInt(temp[1])<=5000){
                word.set(value);
                context.write(word,NullWritable.get());

            }


        }
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text,Text>{
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable tuple = new IntWritable();
        private Text word = new Text();
        private Text value1= new Text();
        private Text value2 = new Text();
        private Text word1 = new Text();

        //map function which takes each line splits it by comma and takes second word in the temp list and emmits that word and 1 as tuple.
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {


            String[] temp =value.toString().split(",");// Splitting the line by comma into a list
            String tupleto = temp[0]+","+temp[1]+",to";
            String tuplefrom =temp[0]+","+temp[1]+",from";
            word.set(temp[0]);
            word1.set(temp[1]);
            value1.set(tuplefrom);
            value2.set(tupleto);
            context.write(word,value1);
            context.write(word1,value2);
            //word.set(temp[1]);// setting word as the second word in the list
            //context.write((temp[0],temp[1]), one);// emmiting the second value in the list with as a tuple

        }
    }

    public static class Job2Mapper1 extends Mapper<Object, Text, Text,Text>{
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable tuple = new IntWritable();
        private Text word = new Text();
        private Text word1 = new Text();

        //map function which takes each line splits it by comma and takes second word in the temp list and emmits that word and 1 as tuple.
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {


            String[] temp =value.toString().split(",");// Splitting the line by comma into a list
            //String tupleto = temp[0]+","+temp[1]+",to";
            String tuplefrom =temp[0]+","+temp[1]+",from";
            word.set(tuplefrom);
            word1.set(temp[0]);
            context.write(word1,word);

            //word.set(temp[1]);// setting word as the second word in the list
            //context.write((temp[0],temp[1]), one);// emmiting the second value in the list with as a tuple

        }
    }

    public static class Job2Mapper2 extends Mapper<Object, Text, Text,Text>{
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable tuple = new IntWritable();
        private Text word = new Text();
        private Text word1 = new Text();

        //map function which takes each line splits it by comma and takes second word in the temp list and emmits that word and 1 as tuple.
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {


            String[] temp =value.toString().split(",");// Splitting the line by comma into a list
            String tupleto = temp[0]+","+temp[2]+",to";
            //String tuplefrom =temp[0]+","+temp[1]+",from";
            word.set(tupleto);
            word1.set(temp[2]);
            context.write(word1,word);

            //word.set(temp[1]);// setting word as the second word in the list
            //context.write((temp[0],temp[1]), one);// emmiting the second value in the list with as a tuple

        }
    }

    public static class Job2Reducer
            extends Reducer<Text,Text,Text,NullWritable> {
        private Text result = new Text();


        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            List<String> to_list = new ArrayList<String>();
            to_list.clear();
            List<String> from_list = new ArrayList<String>();
            from_list.clear();

            for (Text val : values) {
                String[] temp = val.toString().split(",");
                String s =val.toString();
                System.out.println(val);
                System.out.println((temp[2].equals("to")));
                System.out.println((temp[2].equals("from")));


                if (temp[2].equals("to")){

                    to_list.add(s);

                }
                if  ((temp[2].equals("from"))){
                    from_list.add(s);
                }
            }
            if (!to_list.isEmpty() && !from_list.isEmpty()) {
                for (String A : from_list) {
                    for (String B : to_list) {

                        String[] left_row = A.toString().split(",");
                        String[] right_row= B.toString().split(",");

                        if (left_row[1].equals(right_row[0]) && left_row[0].equals(right_row[1]) ){
                            context.getCounter(Count.numtriangles).increment(1);
                            String result1 = left_row[0]+","+left_row[1]+","+right_row[1];
                            result.set(result1);
                            context.write(result,NullWritable.get());
                        }
                    }
                }
            }

        }
    }






    public static class IntSumReducer
            extends Reducer<Text,Text,Text,NullWritable> {
        private Text result = new Text();


        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            List<String> to_list = new ArrayList<String>();
            to_list.clear();
            List<String> from_list = new ArrayList<String>();
            from_list.clear();

            for (Text val : values) {
                String[] temp = val.toString().split(",");
                String s =val.toString();



                if (temp[2].equals("to")){

                    to_list.add(s);

                }
                if  ((temp[2].equals("from"))){
                    from_list.add(s);
                }
            }
            if (!to_list.isEmpty() && !from_list.isEmpty()) {
                for (String A : from_list) {
                    for (String B : to_list) {

                        String[] left_row = A.toString().split(",");
                        String[] right_row= B.toString().split(",");

                        if (left_row[1].equals(right_row[0]) ){
                            String result1 = left_row[0]+","+left_row[1]+","+right_row[1];
                            result.set(result1);
                            System.out.println(result);
                            context.write(result,NullWritable.get());
                        }
                        if (left_row[0].equals(right_row[1]) ){
                            String result1 = right_row[0]+","+right_row[1]+","+left_row[1];
                            result.set(result1);
                            System.out.println(result);
                            context.write(result,NullWritable.get());
                        }
                    }
                }
            }

        }
    }





    //Reduce class which has a reduce function called for each reduce call

    public static void main(String[] args) throws Exception {

        Configuration conf1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();

        //error handling
        if (otherArgs.length < 4) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }


        //Creating a job and passing configuration to it
        Job job1 = Job.getInstance(conf1, "Twitter Follower Count");
        job1.setJarByClass(RSJoin.class);//Assigning the jar class

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


        Configuration conf2 = new Configuration();


        //error handling
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }


        //Creating a job and passing configuration to it
        Job job2 = Job.getInstance(conf2, "Twitter Follower Count");
        job2.setJarByClass(RSJoin.class);//Assigning the jar class

        //job.setReducerClass(IntSumReducer.class);//calling reduce on the combiner output
        final Configuration jobConf = job2.getConfiguration(); //getting job configuration
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
        //job.setMapperClass(RSJoin.MaxFilterMapper.class); //calling map task on the job
        job2.setMapperClass(RSJoin.TokenizerMapper.class); //calling map task on the job
        //job.setPartitionerClass(Partition.class);
        job2.setReducerClass(RSJoin.IntSumReducer.class);

        //writing outputs
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);




        //setting paths

        FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));

        FileOutputFormat.setOutputPath(job2,
                new Path(otherArgs[2]));


        //job.setReducerClass(IntSumReducer.class);//calling reduce on the combiner output

       if (!job2.waitForCompletion(true)){
           System.exit(1);
        }

        //writing outputs

        Configuration conf3 = new Configuration();


        //error handling
        Job job3 = Job.getInstance(conf3, "Twitter Follower Count1");
        job3.setJarByClass(RSJoin.class);//Assigning the jar class

        //job.setReducerClass(IntSumReducer.class);//calling reduce on the combiner output
        final Configuration jobConf3 = job3.getConfiguration(); //getting job configuration
        jobConf3.set("mapreduce.output.textoutputformat.separator", "\t");


        MultipleInputs.addInputPath(job3, new Path(otherArgs[1]),
                TextInputFormat.class, RSJoin.Job2Mapper1.class);

        MultipleInputs.addInputPath(job3, new Path(otherArgs[2]),
                TextInputFormat.class, RSJoin.Job2Mapper2.class);

        job3.setReducerClass(RSJoin.Job2Reducer.class);

        FileOutputFormat.setOutputPath(job3, new Path(otherArgs[3]));
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        System.out.println(job3.waitForCompletion(true) ? 0 : 3);
        Counter c = job3.getCounters().findCounter(Count.numtriangles);
        long final_count = c.getValue();

        final_count/=3;
        System.out.println(final_count);








    }


}


