import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import java.util.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured; 
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
    
    @Override
    public int compareTo(Object o) {
    	 Color co = (Color)o;
         if(co.type!=type){
         return (int) (type-co.type);
       }
       else
       	return (int) (intensity-co.intensity);
	}
    @Override
    public int hashCode(){
    	return (1000*type+intensity);
	}
    @Override
    public boolean equals(Object o) { 
  
        // If the object is compared with itself then return true   
        if (o == this) { 
            return true; 
        } 
  
        /* Check if o is an instance of Complex or not 
          "null instanceof [type]" also returns false */
        if (o==null || getClass()!=o.getClass()) { 
            return false; 
        } 
          
        // typecast o to Color so that we can compare data members  
        Color c = (Color) o; 
          
        // Compare the data members and return accordingly  
        return type==c.type && intensity==c.intensity; 
    } 
}


public class Histogram extends Configured implements Tool{
    public static class HistogramMapper2 extends Mapper<Object,Text,Color,IntWritable> {
        static Hashtable H;
	@Override
	protected void setup ( Context context ) throws IOException,InterruptedException {
		H = new Hashtable<Color,Integer>();
	}
	@Override
	protected void cleanup ( Context context ) throws IOException,InterruptedException {
		Set<Color> keySet = H.keySet();
		for(Color c:keySet){
			int i = (int) H.get(c);
			context.write(c,new IntWritable(i));
		}
		System.out.println(H.size());
	}
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
              Scanner sc = new Scanner(value.toString()).useDelimiter(",");
              Color red = new Color((short)1,sc.nextShort());
              Color green = new Color((short)2,sc.nextShort());
              Color blue = new Color((short)3,sc.nextShort());
              if(H.get(red)==null)
              	H.put(red,1);
              else{
              int i = (int) H.get(red);
              i++;
                 //System.out.println(H.get(red));
              	 H.put(red,i);
              }
              if(H.get(green)==null)
              	H.put(green,1);
              else{
              //System.out.println("BEFORE GET GREEN");
              int i = (int) H.get(green);
              i++;
                // System.out.println(H.get(green));
              	 H.put(green,i);
              }
              if(H.get(blue)==null)
              	H.put(blue,1);
              else{
              //System.out.println("BEFORE GET BLUE");
              int i = (int) H.get(blue);
              i++;
                // System.out.println(H.get(blue));
              	 H.put(blue,i);
              }
       }
  }
                        
                        
                        
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
    public static class HistogramCombiner extends Reducer<Color,IntWritable,Color,IntWritable> {
        @Override
	public void reduce ( Color key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable i : values){
			sum += i.get();
		};
		context.write(key,new IntWritable(sum));
	} 
    }
    public static class HistogramReducer extends Reducer<Color,IntWritable,Color,IntWritable> {
        @Override
        public void reduce ( Color key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
            /* write your reducer code */
		int sum = 0;
		for(IntWritable i : values){
			sum += i.get();
		};
		context.write(key,new IntWritable(sum));
        }
    }
    @Override
    public int run ( String [ ] args ) throws Exception {
        String arg1 = args[0];
        String arg2 = args[1];
        Configuration conf = getConf();
	Job job = Job.getInstance(conf,"Job1");
        job.setJobName("HistogramJob");
	job.setJarByClass(Histogram.class);
        job.setOutputKeyClass(Color.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Color.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(HistogramMapper.class);
        job.setCombinerClass(HistogramCombiner.class);
        job.setReducerClass(HistogramReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(arg1));
        FileOutputFormat.setOutputPath(job,new Path(arg2));
        job.waitForCompletion(true);
        arg2=arg2.concat("2");
        System.out.println(arg2);
        Job job1 = Job.getInstance(conf,"Job2");
        job1.setJobName("HistogramJob2");
	job1.setJarByClass(Histogram.class);
        job1.setOutputKeyClass(Color.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapOutputKeyClass(Color.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setMapperClass(HistogramMapper2.class);
        job1.setReducerClass(HistogramReducer.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job1,new Path(arg1));
        FileOutputFormat.setOutputPath(job1,new Path(arg2));
        job1.waitForCompletion(true);
        return 0;
	}
    public static void main ( String[] args ) throws Exception {
        /* write your main program code */
        ToolRunner.run(new Configuration(),new Histogram(),args);
   }
}
