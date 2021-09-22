package TwitterTriangleMR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RSjoin2 {
    private static final Logger logger = LogManager.getLogger(RSJoin.class);




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
                            String result1 = left_row[0]+","+left_row[1]+","+right_row[1];
                            result.set(result1);
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
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }






        Job job2 = Job.getInstance(conf1, "Twitter Follower Count1");
        job2.setJarByClass(RSJoin.class);//Assigning the jar class

        //job.setReducerClass(IntSumReducer.class);//calling reduce on the combiner output
        final Configuration jobConf2 = job2.getConfiguration(); //getting job configuration
        jobConf2.set("mapreduce.output.textoutputformat.separator", "\t");


        MultipleInputs.addInputPath(job2, new Path(otherArgs[0]),
                TextInputFormat.class, Job2Mapper1.class);

        MultipleInputs.addInputPath(job2, new Path(otherArgs[1]),
                TextInputFormat.class, Job2Mapper2.class);

        job2.setReducerClass(Job2Reducer.class);

        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        System.exit(job2.waitForCompletion(true) ? 0 : 3);

        //writing outputs





    }


}



