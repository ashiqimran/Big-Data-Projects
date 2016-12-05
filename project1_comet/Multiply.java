package edu.uta.cse6331;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;

/*
 * Author: Ashiq Imran
 * */
public class Multiply {

	public static class MatMapper1 extends Mapper<Object, Text, IntWritable, Matrix> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			int i = s.nextInt();
			int j = s.nextInt();
			double val = s.nextDouble();
			short t = 0;
			context.write(new IntWritable(j), new Matrix(t, i, val));
			s.close();
		}

	}

	public static class MatMapper2 extends Mapper<Object, Text, IntWritable, Matrix> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			int j = s.nextInt();
			int k = s.nextInt();
			double val = s.nextDouble();
			short t = 1;
			context.write(new IntWritable(j), new Matrix(t, k, val));
			s.close();
		}

	}

	public static class MatReducer1 extends Reducer<IntWritable, Matrix, Pair, DoubleWritable> {
		static Vector<Matrix> mat1 = new Vector<>();
		static Vector<Matrix> mat2 = new Vector<>();

		@Override
		public void reduce(IntWritable key, Iterable<Matrix> values, Context context)
				throws IOException, InterruptedException {
			mat1.clear();
			mat2.clear();
			for (Matrix v : values) {
				if (v.tag == 0) {
					mat1.add(new Matrix(v.tag, v.index, v.value));

				} else {
					mat2.add(new Matrix(v.tag, v.index, v.value));
				}
			}

			for (Matrix m : mat1) {
				for (Matrix n : mat2) {
					context.write(new Pair(m.index, n.index),
							new DoubleWritable( m.value * n.value));

				}
			}

		}
	}

	public static class MatMapper3 extends Mapper<Pair, DoubleWritable, Pair, DoubleWritable> {
		@Override
		public void map(Pair p, DoubleWritable value, Context context) throws IOException, InterruptedException {
			context.write(p, value);
		}

	}

	public static class MatReducer2 extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {

		@Override
		public void reduce(Pair p, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			double m = 0.0d;
			for (DoubleWritable v : values) {
				m += v.get();
			}
			context.write(p, new DoubleWritable(m));

		}
	}

	public static void main(String[] args)
			throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {


		Job job = Job.getInstance();
		job.setJobName("Multiply");
		job.setJarByClass(Multiply.class);
		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Matrix.class);
		job.setReducerClass(MatReducer1.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MatMapper1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MatMapper2.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
		Path intermediate = new Path(args[2]);
		FileOutputFormat.setOutputPath(job, intermediate);
		job.waitForCompletion(true);

		Job job2 = Job.getInstance();
		job2.setJobName("Summation");
		job2.setJarByClass(Multiply.class);

		job2.setMapOutputKeyClass(Pair.class);
		job2.setMapOutputValueClass(DoubleWritable.class);
		job2.setOutputKeyClass(Pair.class);
		job2.setOutputValueClass(DoubleWritable.class);

		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setMapperClass(MatMapper3.class);
		job2.setReducerClass(MatReducer2.class);
		FileInputFormat.setInputPaths(job2, intermediate);
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		job2.waitForCompletion(true);

	}

}

class Matrix implements Writable {
	public short tag;
	public int index;
	public double value;

	Matrix() {

	}

	Matrix(short t, int r, double v) {
		tag = t;
		index = r;
		value = v;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeShort(tag);
		out.writeInt(index);
		out.writeDouble(value);

	}

	@Override
	public void readFields(DataInput in) throws IOException {

		tag = in.readShort();
		index = in.readInt();
		value = in.readDouble();

	}

}

class Pair implements WritableComparable<Pair> {

	public int i;
	public int j;

	Pair() {

	}

	Pair(int i, int j) {
		this.i = i;
		this.j = j;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(i);
		out.writeInt(j);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		i = in.readInt();
		j = in.readInt();
	}

	@Override
	public int compareTo(Pair o) {
		return i == o.i ? j - o.j : i - o.i;

	}

	@Override
	public String toString() {
		return i + "," + j;
	}

}


