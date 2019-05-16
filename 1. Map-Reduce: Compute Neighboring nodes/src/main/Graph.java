import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.Scanner;


public class Graph extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments   files\n",
                    getClass().getSimpleName());
            return -1;
        }

        Configuration conf = new Configuration();

        // Job using First mapper and reducer
        Job job = Job.getInstance(conf, "Graph 1");
        job.setJarByClass(Graph.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MyMapperOne.class);
        job.setReducerClass(MyReducerOne.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("itempd"));
        //FileOutputFormat.setCompressOutput(job, true);
        //FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        //SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        job.waitForCompletion(true);

        System.out.println("Job 2 Started");
	    // Job using second mapper and reducer

        job = Job.getInstance(conf, "Graph 2");
        job.setJarByClass(Graph.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MyMapperTwo.class);
        job.setReducerClass(MyReducerTwo.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("itempd/part-r-00000"));// /part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ?1:0);
        return 0;
    }
    public static class MyMapperOne extends Mapper<Object ,Text,IntWritable,IntWritable> {

        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int x = Integer.parseInt(s.next().toString());
            int y = Integer.parseInt(s.next().toString());
            context.write(new IntWritable(x),new IntWritable(y));
            s.close();
        }
    }

    public static class MyReducerOne extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        @Override
        public void reduce ( IntWritable key, Iterable<IntWritable> values, Context context )
                throws IOException, InterruptedException {
            int count=0;
            for (IntWritable v: values) {
                count++;
            };
            context.write(key,new IntWritable(count));
        }
    }

    public static class MyMapperTwo extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
	Scanner scanner= new Scanner(value.toString()).useDelimiter("\t");
	int x= scanner.nextInt();
	int y=scanner.nextInt();
	IntWritable one = new IntWritable(1);	
	context.write(new IntWritable(y),one );
     	scanner.close();
	   }

    }

    public static class MyReducerTwo extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            	int count= 0;
		for(IntWritable value: values)
                	count++;
	   	context.write(key,new IntWritable(count));
        }
	}
   

    public static void main ( String[] args ) throws Exception {

        System.out.println("This is the output");
     	int exitCode = ToolRunner.run(new Graph(), args);
        System.exit(exitCode);
    

   }
}
