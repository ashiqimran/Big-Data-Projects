package edu.uta.cse6331;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/*
 * Author: Ashiq Imran
 */

public class Graph extends Configured implements Tool {

	public static class Mapper1 extends Mapper<Object, Text, LongWritable, Vertex> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			long vid = s.nextLong();
			ArrayList<Long> adj = new ArrayList<>();

			while (s.hasNextLong()) {
				adj.add(s.nextLong());

			}
			short t = 0;
			context.write(new LongWritable(vid), new Vertex(t, vid, vid, adj,adj.size()));
			s.close();
		}

	}

	// public static class Reducer1 extends Reducer<LongWritable, Vertex,
	// LongWritable, Vertex> {
	//
	// @Override
	// public void reduce(LongWritable key, Iterable<Vertex> values, Context
	// context)
	// throws IOException, InterruptedException {
	// //
	//
	// }
	// }

	public static class Mapper2 extends Mapper<Object, Vertex, LongWritable, Vertex> {
		@Override
		public void map(Object key, Vertex value, Context context) throws IOException, InterruptedException {

			context.write(new LongWritable(value.vid), value);

			for (long n : value.adjacent) {
				context.write(new LongWritable(n), new Vertex((short) 1, value.group));
			}
		}

	}

	public static class Reducer2 extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {

		@Override
		public void reduce(LongWritable key, Iterable<Vertex> values, Context context)
				throws IOException, InterruptedException {
			long m = Long.MAX_VALUE;
			long vid = key.get();
			Vertex ver = null;
			for (Vertex v : values) {
				if (v.tag == 0) {
					ver = new Vertex(v.tag,v.group,v.vid,v.adjacent,v.size);
				}
				m = Math.min(m, v.group);
			}
			context.write(new LongWritable(m), new Vertex((short) 0, m, vid, ver.adjacent,ver.size));

		}
	}

	public static class Mapper3 extends Mapper<LongWritable, Vertex, LongWritable, LongWritable> {
		@Override
		public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException {

			context.write(key, new LongWritable(1));
		}

	}

	public static class Reducer3 extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

		@Override
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long m = 0;
			for (LongWritable v : values) {
				m = m + v.get();
			}
			context.write(key, new LongWritable(m));

		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "Job1");
		job.setJobName("Graph");
		job.setJarByClass(Graph.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Vertex.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Vertex.class);
		job.setMapperClass(Mapper1.class);
		// job.setReducerClass(Reducer1.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path intermediate = new Path(args[1] + "/f0");
		FileOutputFormat.setOutputPath(job, intermediate);
		job.waitForCompletion(true);

		for (int i = 0; i < 5; i++) {
			Job job2 = Job.getInstance(conf, "Job2");
			job2.setJarByClass(Graph.class);
			job2.setOutputKeyClass(LongWritable.class);
			job2.setOutputValueClass(Vertex.class);
			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(Vertex.class);
			job2.setMapperClass(Mapper2.class);
			job2.setReducerClass(Reducer2.class);
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(SequenceFileOutputFormat.class);
			intermediate = new Path(args[1] + "/f" + i);
			FileInputFormat.setInputPaths(job2, intermediate);
			intermediate = new Path(args[1] + "/f" + (i + 1));
			FileOutputFormat.setOutputPath(job2, intermediate);
			job2.waitForCompletion(true);
		}

		Job job3 = Job.getInstance(conf, "Job3");
		job3.setJarByClass(Graph.class);
		job3.setOutputKeyClass(LongWritable.class);
		job3.setOutputValueClass(LongWritable.class);
		job3.setMapOutputKeyClass(LongWritable.class);
		job3.setMapOutputValueClass(LongWritable.class);
		job3.setMapperClass(Mapper3.class);
		job3.setReducerClass(Reducer3.class);
		job3.setInputFormatClass(SequenceFileInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		intermediate = new Path(args[1] + "/f5");
		FileInputFormat.setInputPaths(job3, intermediate);
		FileOutputFormat.setOutputPath(job3, new Path(args[2]));
		job3.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ToolRunner.run(new Configuration(), new Graph(), args);

	}

}

class Vertex implements Writable{
	short tag;
	long group;
	long vid;
	long size;
	ArrayList<Long> adjacent;

	Vertex() {

	}

	Vertex(short tag, long group) {
		this.tag = tag;
		this.group = group;
	}

	Vertex(short tag, long group, long vid, ArrayList<Long> adj, long size) {
		this.tag = tag;
		this.group = group;
		this.vid = vid;
		this.adjacent = new ArrayList<Long>(adj);
		this.size = size;
		//System.out.println("ADJACENT " + adjacent.get(0));
	}

	@Override
	public void write(DataOutput out) throws IOException {

		// TODO Auto-generated method stub
		out.writeShort(tag);
		out.writeLong(group);
		out.writeLong(vid);
		out.writeLong(size);
		for (int i = 0; i < size; i++) {
			out.writeLong(adjacent.get(i));
		}

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		tag = in.readShort();
		group = in.readLong();
		vid = in.readLong();
		size = in.readLong();
		adjacent = new ArrayList<Long>((int) size);
		for (int i = 0; i < size; i++) {
		        adjacent.add(in.readLong());
		}

	}


	@Override
	public String toString() {
		return tag + " " + group + " " + vid + " " + adjacent;
	}

}
