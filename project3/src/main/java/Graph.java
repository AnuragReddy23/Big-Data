import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.util.Vector;


class Vertex implements Writable {
    public short flag;           // 0 for a graph vertex, 1 for a grp number
    public long grp;          // the group where this vertex belongs to
    public long Ver_id;            // the vertex ID
    public Vector<Long> adj;     // the vertex neighbors
    /* ... */

Vertex(short flag,long grp,long Ver_id,Vector adj)
{
    this.flag=flag;
    this.grp=grp;
    this.Ver_id=Ver_id;
    this.adj=adj;
}
Vertex()
{ 
}
Vertex(short flag,long grp)
{
    this.flag=flag;
    this.grp=grp;
    this.Ver_id=0;
    this.adj=new Vector();
} 
public void print()
{
    System.out.println(flag+","+grp+","+Ver_id+","+adj);
}
public void write ( DataOutput out ) throws IOException {
    out.writeShort(this.flag);
    out.writeLong(this.grp);
    out.writeLong(this.Ver_id);
    out.writeInt(adj.size());
for(int i=0;i<adj.size();i++)
{ 
out.writeLong(adj.get(i));
}
}
public void readFields ( DataInput in ) throws IOException {
flag=in.readShort();
grp=in.readLong();
Ver_id=in.readLong();
int j=in.readInt(); 
Vector<Long> v=new Vector();
for(int i=0;i<j;i++)
{
long temp=in.readLong(); 
v.addElement(temp);
}
adj=v;
} 
}


public class Graph {
public static class map1 extends Mapper<Object,Text,LongWritable,Vertex> {
    @Override
    public void map ( Object key, Text value, Context context )
    throws IOException, InterruptedException {
        Vector<Long> v =new Vector();
        Scanner s = new Scanner(value.toString()).useDelimiter(",");
        int j=0;
        Integer z=0;
        long ver_id=0;
        Integer iver_id=0;
        while(s.hasNext())
        {
        iver_id=s.nextInt();
        if(j==0)
        {
        ver_id=iver_id.longValue();
        j=1;
        }
        else
        {
        long aver_id=iver_id.longValue();
        v.add(aver_id);
        } 
        }
        // System.out.println();
        Vertex v1=new Vertex(z.shortValue(),ver_id,ver_id,v); 
        context.write(new LongWritable(ver_id),v1);
        s.close();
        }
    }
    public static class map2 extends Mapper<LongWritable,Vertex,LongWritable,Vertex> { 
    public void map ( LongWritable key, Vertex v, Context context )
    throws IOException, InterruptedException { 

        context.write(new LongWritable(v.Ver_id),v);
        int size=v.adj.size();
        for(int i=0;i<size;i++)
        {

        short s=1;
        context.write(new LongWritable(v.adj.get(i)),new Vertex(s,v.grp));
        }
        }
        }
        public static long min(long a,long b)
        {
        if(a<b){
        return a;
        }
        else{
        return b;
        }
        }
        public static class red1 extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
        public void reduce ( LongWritable ver_id, Iterable<Vertex> values, Context context)
        throws IOException, InterruptedException {
        long m=Long.MAX_VALUE;
        Vector<Long> adj =new Vector();
        for(Vertex v:values) 
        {
        if(v.flag==0)
        {
        adj=v.adj;
        }
        m=min(m,v.grp);
        }
        short s=0;
        context.write(new LongWritable(m),new Vertex(s,m,ver_id.get(),adj));
        }
        }
        public static class map3 extends Mapper<LongWritable,Vertex,LongWritable,LongWritable> { 
        public void map ( LongWritable key, Vertex v, Context context )
        throws IOException, InterruptedException {
        context.write(key,new LongWritable(1));
        }
        }
        public static class red3 extends 
        Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
        long m=0;
        for(LongWritable v:values) 
        {
        m=m+v.get();
        } 
        context.write(key,new LongWritable(m));
        }
        }
public static void main ( String[] args ) throws Exception {
        Job job1 = Job.getInstance();
        job1.setJobName("Job1");
        job1.setJarByClass(Graph.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Vertex.class);
        job1.setMapperClass(map1.class); 
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job1,new Path(args[0])); 
        FileOutputFormat.setOutputPath(job1,new Path(args[1]+"/f0"));
        job1.waitForCompletion(true);
        for ( short i = 0; i < 5; i++ ) {
        Job job2 = Job.getInstance();
        job2.setJobName("Job2");
        job2.setJarByClass(Graph.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Vertex.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Vertex.class);
        job2.setMapperClass(map2.class); 
        job2.setReducerClass(red1.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path(args[1]+"/f"+i)); 
        FileOutputFormat.setOutputPath(job2,new Path(args[1]+"/f"+(i+1)));
        job2.waitForCompletion(true);
        }
        Job job3 = Job.getInstance();
        job3.setJobName("Job3");
        job3.setJarByClass(Graph.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(Vertex.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);
        job3.setMapperClass(map3.class); 
        job3.setReducerClass(red3.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job3,new Path(args[1]+"/f5")); 
        FileOutputFormat.setOutputPath(job3,new Path(args[2]));
        job3.waitForCompletion(true);
        }
}

