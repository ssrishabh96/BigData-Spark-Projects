import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Scanner;
import java.util.Vector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Vertex implements Writable {
    short tag;
    long group;
    long VID;
    Vector<Long> adjacent = new Vector<Long>();
    
    Vertex() {}
    
    Vertex(short a, long b, long c, Vector<Long> d) {
        tag = a;
        group = b;
        VID = c;
        adjacent = d;
    }
    
    Vertex(short i, long j) {
        tag = i;
        group = j;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tag = in.readShort();
        group = in.readLong();
        VID = in.readLong();
        
        int len = in.readInt();
        Vector<Long> adj = new Vector<Long>();
        for(int x =0; x< len; x++) {
            adj.add(in.readLong());
        }
        adjacent = adj;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeShort(tag);
        out.writeLong(group);
        out.writeLong(VID);
        
        out.writeInt(adjacent.size());
        for(int x = 0; x < adjacent.size(); x++) {
            out.writeLong(adjacent.get(x));
        }
    }
    
    @Override
    public String toString() {
        return tag+","+group+","+VID+","+adjacent;
        }
}

public class Graph {
    
    public static class FirstMapper extends Mapper<Object,Text,LongWritable,Vertex> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            Vector<Long> nodes  = new Vector<Long>();
            Long VID = s.nextLong();
            while(s.hasNext()) {
                nodes.add(s.nextLong());
            }
            context.write(new LongWritable(VID), new Vertex((short) 0, VID, VID, nodes));
            s.close();
        }
    }
    
    public static class SecondMapper extends Mapper<LongWritable,Vertex,LongWritable,Vertex> {
        @Override
        public void map ( LongWritable key, Vertex value, Context context )
                        throws IOException, InterruptedException {
            context.write(new LongWritable(value.VID), value);
            for(Long n : value.adjacent) {
                context.write(new LongWritable(n), new Vertex((short)1, value.group));
            }
        }
    }
    
    public static class SecondReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
        Vector<Long> adj = new Vector<Long>();
        @Override
        public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
                           throws IOException, InterruptedException {
            long m = Long.MAX_VALUE;
            for(Vertex v : values) {
                if(v.tag == 0) {
                    adj = (Vector<Long>) v.adjacent.clone();
                }
                m = Math.min(m, v.group);
            }
            context.write(new LongWritable(m), new Vertex((short)0, m, key.get(), adj));
        }
    }
    
    public static class ThirdMapper extends Mapper<LongWritable,Vertex,LongWritable,LongWritable> {
        @Override
        public void map ( LongWritable key, Vertex value, Context context )
                        throws IOException, InterruptedException {
            context.write(key, new LongWritable(1));
            }
        }
    
    public static class ThirdReducer extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context )
                           throws IOException, InterruptedException {
            Long m = (long) 0;
            for(LongWritable v : values) {
                m = m + v.get();
            }
            context.write(key, new LongWritable(m));
        }
    }
    
    public static void main ( String[] args ) throws Exception {
        //Job1
        Job job1 = Job.getInstance();
        job1.setJobName("Graph-processing-Stage1");
        job1.setJarByClass(Graph.class);
        
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);
        
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Vertex.class);
        
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,FirstMapper.class);
        FileOutputFormat.setOutputPath(job1,new Path(args[1]+"/f0"));
        job1.waitForCompletion(true);
        
        //Job2  
        for(int x = 0; x < 5; x++) {  
            Job job2 = Job.getInstance();
            job2.setJobName("Graph-processing-Stage2");
            job2.setJarByClass(Graph.class);
            
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Vertex.class);
            
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Vertex.class);
            job2.setReducerClass(SecondReducer.class);
            
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            MultipleInputs.addInputPath(job2,new Path(args[1]+"/f"+x),SequenceFileInputFormat.class,SecondMapper.class);
            FileOutputFormat.setOutputPath(job2,new Path(args[1]+"/f"+(x+1)));
            job2.waitForCompletion(true);
        }
        
        //Job3
        Job job3 = Job.getInstance();
        job3.setJobName("Graph-processing-Stage3");
        job3.setJarByClass(Graph.class);
        
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(LongWritable.class);
        
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);
        job3.setReducerClass(ThirdReducer.class);
        
        job3.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job3,new Path(args[1]+"/f5"),SequenceFileInputFormat.class,ThirdMapper.class);
        FileOutputFormat.setOutputPath(job3,new Path(args[2]));
        job3.waitForCompletion(true);       
    }
}