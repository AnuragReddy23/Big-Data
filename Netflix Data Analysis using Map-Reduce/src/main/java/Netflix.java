import java.io.*;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Netflix {
public static class MyMapper extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
           
            String user = s.next();
int user_id = 0;
int rating = 0;            
            if (user.contains(":") == false)
            { 

                user_id = Integer.parseInt(user);
                rating = s.nextInt();
                String date = s.next();
                context.write(new IntWritable(user_id),new IntWritable(rating));
                
            }
            s.close();
        }
    }

    public static class MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,DoubleWritable> {
        @Override
        public void reduce ( IntWritable key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (IntWritable v: values) {
                sum += v.get();
                count++;
            };
            context.write(key,new DoubleWritable( (int)(sum/count*10)));
        }
    }


    public static class MyMapper2 extends Mapper<Object,Text,DoubleWritable,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter("\t");
            int user_id = s.nextInt();
            double user_rating = s.nextDouble();
            context.write(new DoubleWritable(user_rating/10.0),new IntWritable(1));
            s.close();
        }
    }

    public static class MyReducer2 extends Reducer<DoubleWritable,IntWritable,DoubleWritable,IntWritable> {
        @Override
        public void reduce ( DoubleWritable key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v: values) {
                sum += v.get();
            };
            context.write(key,new IntWritable(sum));
        }
    }



    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("First_Job");
        job.setJarByClass(Netflix.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);


        Job job1 = Job.getInstance();
        job1.setJobName("Second_MapReduce");
        job1.setJarByClass(Netflix.class);
        job1.setOutputKeyClass(DoubleWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapOutputKeyClass(DoubleWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setMapperClass(MyMapper2.class);
        job1.setReducerClass(MyReducer2.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job1,new Path(args[1]));
        FileOutputFormat.setOutputPath(job1,new Path(args[2]));
        job1.waitForCompletion(true);
    }
}