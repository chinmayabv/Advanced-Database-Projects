//Name:Chinmaya Venkatesh
//UTA ID:1001778014

import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


/* single color intensity */
class Color implements WritableComparable {
    public short type;       /* red=1, green=2, blue=3 */
    public short intensity;  /* between 0 and 255 */
    /* need class constructors, toString, write, readFields, and compareTo methods */
    Color(){}
    
    Color(short ty,short intens){
	type = ty;
	intensity = intens;
    }
    public void write ( DataOutput out ) throws IOException {
        out.writeShort(type);
        out.writeShort(intensity);
    }

    public void readFields ( DataInput in ) throws IOException {
        type = in.readShort();
        intensity = in.readShort();
    }
    
    @Override
    public String toString() { return type+" "+intensity; }
    
    public int compareTo(Object o) {
	 Color co = (Color)o;
         short thisType = this.type;
         short thatType = co.type;
         short thisIntense = this.intensity;
         short thatIntense = co.intensity;
         if(thisType==thatType){
         	if(thisIntense == thatIntense){
         		return 0;         	
         	}
         	else if(thisIntense>thatIntense){
    			return 1;
    		}
    		else
    			return -1;
         }
         else if(thisType<thatType){
         	return -1;
         }
         return 1;
       }
}


public class Histogram {
    public static class HistogramMapper extends Mapper<Object,Text,Color,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            /* write your mapper code */
		Scanner sc = new Scanner(value.toString()).useDelimiter(",");
		short red = sc.nextShort();
		short green = sc.nextShort();
		short blue = sc.nextShort();
		context.write(new Color((short)1,red),new IntWritable(1));
		context.write(new Color((short)2,green),new IntWritable(1));
		context.write(new Color((short)3,blue),new IntWritable(1));
		sc.close();
        }
    }

    public static class HistogramReducer extends Reducer<Color,IntWritable,Color,IntWritable> {
        @Override
        public void reduce ( Color key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            /* write your reducer code */
		int sum = 0;
		for(IntWritable i : values){
			sum += i.get();
		};
		context.write(key,new IntWritable(sum));
        }
    }

    public static void main ( String[] args ) throws Exception {
        /* write your main program code */
        Job job = Job.getInstance();
        job.setJobName("HistogramJob");
	job.setJarByClass(Histogram.class);
        job.setOutputKeyClass(Color.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Color.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(HistogramMapper.class);
        job.setReducerClass(HistogramReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);
   }
}
