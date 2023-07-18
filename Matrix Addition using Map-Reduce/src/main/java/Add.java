import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Triple implements Writable {
    public int i;
    public int j;
    public double value;
	
    Triple () {}
	
    Triple ( int i, int j, double v ) { this.i = i; this.j = j; value = v; }
	
    public void write ( DataOutput out ) throws IOException {
    	out.writeInt(i);
    	out.writeInt(j);
    	out.writeDouble(value);
    }

    public void readFields ( DataInput in ) throws IOException {
		i = in.readInt();
		j = in.readInt();
		value = in.readDouble();
    }

    @Override
    public String toString () {
        return ""+i+"\t"+j+"\t"+value;
    }
}

class Block implements Writable {
    final static int rows = 100;
    final static int columns = 100;
    public double[][] data;

    Block () {
        data = new double[rows][columns];
    }

    public void write ( DataOutput out ) throws IOException {
    	out.writeInt(rows);
    	out.writeInt(columns);
    }

    public void readFields ( DataInput in ) throws IOException {
    	in.readInt();
    	in.readInt();
    }

    @Override
    public String toString () {
        String s = "\n";
        for ( int i = 0; i < rows; i++ ) {
            for ( int j = 0; j < columns; j++ )
                s += String.format("\t%.3f",data[i][j]);
            s += "\n";
        }
        return s;
    }
}

class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;
	
    Pair () {}
	
    Pair ( int i, int j ) { this.i = i; this.j = j; }
	
    public void write ( DataOutput out ) throws IOException {
    	out.writeInt(i);
    	out.writeInt(j);
    }

    public void readFields ( DataInput in ) throws IOException {
		i = in.readInt();
		j = in.readInt();
    }

    @Override
    public int compareTo ( Pair o ) {
		if (i > o.i) {
			return 1;
		} else if ( i < o.i) {
			return -1;
		} else {
			if(j > o.j) {
				return 1;
			} else if (j < o.j) {
				return -1;
			}
		}
		return 0;
    }

    @Override
    public String toString () {
        return ""+i+"\t"+j;
    }
}

public class Add extends Configured implements Tool {
    final static int rows = Block.rows;
    final static int columns = Block.columns;


    @Override
    public int run ( String[] args ) throws Exception {

        
        Job job = Job.getInstance();
        job.setJobName("Job1");
	 	job.setJarByClass(Add.class);
	 		
	 	FileInputFormat.setInputPaths(job,new Path(args[1]));
	 	
		job.setMapperClass(map1.class);
	 	job.setReducerClass(red1.class);
	 		
	 	job.setMapOutputKeyClass(Pair.class);
	 	job.setMapOutputValueClass(Triple.class);
	 		
	 	job.setOutputKeyClass(Pair.class);
	 	job.setOutputValueClass(Block.class);
	 		
	 	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	 		
	 	FileOutputFormat.setOutputPath(job, new Path(args[2]));
	 		
    	job.waitForCompletion(true);
        
        Job job1 = Job.getInstance();
        job1.setJobName("Job1");
	 	job1.setJarByClass(Add.class);
	 		
	 	FileInputFormat.setInputPaths(job1,new Path(args[0]));
	 	
	 	job1.setMapperClass(map1.class);
		job1.setReducerClass(red1.class);
	 		
	 	job1.setMapOutputKeyClass(Pair.class);
	 	job1.setMapOutputValueClass(Triple.class);
	 		
	 	job1.setOutputKeyClass(Pair.class);
	 	job1.setOutputValueClass(Block.class);
	 		
	 	job1.setOutputFormatClass(SequenceFileOutputFormat.class);
	 		
	 	FileOutputFormat.setOutputPath(job1, new Path(args[3]));
	 		
    	job1.waitForCompletion(true);


	 	Job job2 = Job.getInstance();
	 	job2.setJobName("Job2");
	 	job2.setJarByClass(Add.class);
	 		
	 	MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, mapM.class);
	 	MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, mapN.class);
	 	job2.setReducerClass(red2.class);
	 		
	 	job2.setMapOutputKeyClass(Pair.class);
	 	job2.setMapOutputValueClass(Block.class);
	 		
	 	job2.setOutputKeyClass(Pair.class);
	 	job2.setOutputValueClass(Block.class);
	 		
	 	job2.setOutputFormatClass(TextOutputFormat.class);
	 		
	 	FileOutputFormat.setOutputPath(job2, new Path(args[4]));
	 		
    	boolean b = job2.waitForCompletion(true);
        if(b)
    	return 0;
        else
        return 1;
    }

	public static class map1 extends Mapper<Object,Text,Pair,Triple> {
			
        @Override
        public void map(Object key, Text line, Context context)
                throws IOException, InterruptedException {
            Scanner s = new Scanner(line.toString()).useDelimiter(",");
            int i = s.nextInt();
            int j = s.nextInt();
            double v = s.nextDouble();
            context.write(new Pair(i/rows,j/columns), new Triple(i%rows,j%columns,v));
        }
    }

    public static class red1 extends Reducer<Pair,Triple, Pair, Block> {
		
		@Override
		public void reduce(Pair key, Iterable<Triple> triples, Context context) 
				throws IOException, InterruptedException {
			
			// ArrayList<Triple> M = new ArrayList<Triple>();
			// ArrayList<Triple> N = new ArrayList<Triple>();
			// ArrayList<Block> B = new ArrayList<Block>();
			// Configuration con = context.getConfiguration();
			Block b = new Block();
			for(Triple t : triples) {
					
	            b.data[t.i][t.j] = t.value;

                }    
                context.write(key, b);
		}
	}

	public static class mapM extends Mapper<Pair,Block,Pair,Block> {
			
			@Override
			public void map(Pair key, Block value, Context context)
					throws IOException, InterruptedException {
				// String s = value.toString();
				// String[] s1 = s.split(",");
				
				// int in = Integer.parseInt(s1[0]);
				// double d = Double.parseDouble(s1[2]);
                int a = key.i;
				context.write(new Pair(0,a), value);
			}
		}
	
        public static class mapN extends Mapper<Pair,Block,Pair,Block> {
			
			@Override
			public void map(Pair key, Block value, Context context)
					throws IOException, InterruptedException {
				// String s = value.toString();
				// String[] s1 = s.split(",");
				
				// int in = Integer.parseInt(s1[0]);
				// double d = Double.parseDouble(s1[2]);
                int a = key.i;
				context.write(new Pair(1,a), value);
			}
		}
	
		
		public static class red2 extends Reducer<Pair,Block, Pair, Block> {
		
		@Override
		public void reduce(Pair key, Iterable<Block> blocks, Context context) 
				throws IOException, InterruptedException {
			
			ArrayList<Triple> M = new ArrayList<Triple>();
			ArrayList<Triple> N = new ArrayList<Triple>();
			Block B = new Block();

			Configuration con = context.getConfiguration();
			
			for(Block b : blocks) {
				
                Triple add = ReflectionUtils.newInstance(Triple.class, con);
				ReflectionUtils.copy(con, b, add);

                if(key.i==0){
                    M.add(add);
                }
                else if(key.i==1){
                    N.add(add);
                }
				
            }
            for(int i=0;i<M.size();i++) {
				for(int j=0;j<N.size();j++) {
					
					B.data[i][j] = M.get(i).value + N.get(j).value;
	
					context.write(key, B);
					}
				}
			}
        }


    public static void main ( String[] args ) throws Exception {
    	ToolRunner.run(new Configuration(),new Add(),args);
        //run();
    	
    }

    // public static void run(){
    //     System.out.println("Build Successful");
    //     System.out.println("Loading Matrix M and Matrix N");
    //     System.out.println("First Map-Reduce Completed");
    //     System.out.println("Loading Input for Second Map-Reduce");
    //     System.out.println("Second Map-Reduce Completed");
    //     System.out.println("Matrix Addition Completed");

    // }
    
}
