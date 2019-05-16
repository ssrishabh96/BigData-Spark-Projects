import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.jets3t.service.multi.event.DownloadObjectsEvent;
/*
 * args[0] is the first input matrix M,
 * args[1] is the second input matrix N,
 * args[2] is the directory name to pass the intermediate results from the first Map-Reduce job to the second and,
 * args[3] is the output directory.
 */
public class Multiply {

    static class Elem implements Writable{

        Elem(){}

        short tag; // 0 for matrix M, 1 for matrix N
        int index=0; // one of the indexes
        double value=0;

        public Elem(short tag, int index, double value) {
            this.tag = tag;
            this.index = index;
            this.value = value;
        }

        public short getTag() {
            return tag;
        }

        public void setTag(short tag) {
            this.tag = tag;
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public double getValue() {
            return value;
        }

        public void setValue(double value) {
            this.value = value;
        }

        public void write(DataOutput dataOutput) throws IOException {

            dataOutput.writeShort(tag);
            dataOutput.writeInt(index);
            dataOutput.writeDouble(value);
        }

        public void readFields(DataInput dataInput) throws IOException {

            tag = dataInput.readShort();
            index = dataInput.readInt();
            value = dataInput.readDouble();
        }

        @Override
        public String toString() {
            return "tag: " + tag + ", index:" + index + ", value: " + value;
        }
    }

    static class Pair implements WritableComparable<Pair> {
        int i=0, j=0;

        Pair(){}

        public Pair(int index, int index1) {
            this.i=index;
            this.j=index1;
        }

        public int compareTo(Pair pair) {
           if (pair.i == this.i && pair.j == this.j) return 0;

            if (pair.i > this.i) {
                return 1;
            }

            if (pair.i < this.i) {
                return -1;
            }

            if (pair.i == this.i) {
                if (pair.j > this.j)
                    return 1;
                if (pair.j < this.j)
                    return -1;
            }
            return -1;
        }

        public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(i);
        dataOutput.writeInt(j);
        }

        public void readFields(DataInput dataInput) throws IOException {

            i=dataInput.readInt();
            j=dataInput.readInt();
        }

         @Override
        public boolean equals(Object obj) {

            if (obj instanceof Pair){
                Pair pair= (Pair) obj;
                return this.i==pair.i && this.j==pair.j;
            }
            return false;
        }

        @Override
        public String toString() {
        	return this.i + "," + this.j;
    }}

    public static class MatrixMMapper extends Mapper<Object,Text,IntWritable,Elem> {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            String line = value.toString();
            int indexI=Integer.parseInt(line.split(",")[0]);
            int indexJ= Integer.parseInt(line.split(",")[1]);
            double data = Double.parseDouble(line.split(",")[2]);
            context.write(new IntWritable(indexJ),new Elem((short)0, indexI, data));
        }
    }

    public static class MatrixNMapper extends Mapper<Object,Text,IntWritable,Elem> {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            String line = value.toString();
            int indexI=Integer.parseInt(line.split(",")[0]);
            int indexJ= Integer.parseInt(line.split(",")[1]);
            double data = Double.parseDouble(line.split(",")[2]);
            context.write(new IntWritable(indexI),new Elem((short)1, indexJ, data));
        }
    }

    public static class MyFirstReducer extends Reducer<IntWritable,Elem,Pair,DoubleWritable> {
        @Override
        public void reduce ( IntWritable key, Iterable<Elem> values, Context context )
                throws IOException, InterruptedException {
            ArrayList<Elem> Alist=new ArrayList<>();
            ArrayList<Elem> Blist=new ArrayList<>();

         

            for(Elem a: Alist){
                for (Elem b: Blist){
                    context.write(new Pair(a.index, b.index),new DoubleWritable(a.value*b.value));
                }
            }

	// ArrayList<Elem> matAElements = new ArrayList<>();

 //    ArrayList<Elem> matBElements = new ArrayList<>();

            values.forEach(value -> {
                if (value.tag == 0) Alist.add(new Elem(value.tag, value.index, value.value));
                if (value.tag == 1) Blist.add(new Elem(value.tag, value.index, value.value));

            });
            Alist.forEach(matAElement -> {
                Blist.forEach(matBElement -> {
                    try {
                        context.write(new Pair(matAElement.index, matBElement.index), new DoubleWritable(matAElement.value * matBElement.value));
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            });
        }
    }

    public static class MySecondMapper extends Mapper<Object,Object,Pair,DoubleWritable> {
        @Override
        public void map ( Object key, Object value, Context context )
                throws IOException, InterruptedException {
            String line = value.toString();
            String key1 = line.split("\\t")[0];
            String value1 = line.split("\\t")[1];
            int i, j;
            i = Integer.parseInt(key1.split(",")[0]);
            j = Integer.parseInt(key1.split(",")[1]);
            Pair pair = new Pair(i, j);

            double valuee = Double.parseDouble(value1);
            context.write(pair, new DoubleWritable(valuee));
        }
    }

    public static class MySecondReducer extends Reducer<Pair,DoubleWritable,Pair,DoubleWritable> {
        @Override
        public void reduce ( Pair key, Iterable<DoubleWritable> values, Context context )
                throws IOException, InterruptedException {

            double sum=0;
            for (DoubleWritable value: values) {
                sum += value.get();
            };
            context.write(key, new DoubleWritable(sum));
        }
    }


    public static void main ( String[] args ) throws Exception {

        // First Job:
        Job job = Job.getInstance();
        job.setJobName("Matrix_Multiplication_Job");
        job.setJarByClass(Multiply.class);
        job.setReducerClass(MyFirstReducer.class);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,MatrixMMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,MatrixNMapper.class);
        FileOutputFormat.setOutputPath(job,new Path(args[2]));

        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Elem.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.waitForCompletion(true);

        System.out.println("Starting the Second MR-Job");
        job = Job.getInstance();
        job.setJobName("Matrix_Multiplication_Job");
        job.setJarByClass(Multiply.class);
        job.setMapperClass(MySecondMapper.class);
        job.setReducerClass(MySecondReducer.class);

        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapOutputKeyClass(Pair.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[2]));
        FileOutputFormat.setOutputPath(job,new Path(args[3]));
        job.waitForCompletion(true);

    }
}
