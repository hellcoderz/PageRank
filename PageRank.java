import java.io.File;
import java.io.IOException;
import java.util.*;
import java.io.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRank{



 public static void deleteDir(File dir) throws IOException {
                if (!dir.isDirectory()) {
                        return ;
                }

                File[] files = dir.listFiles();
                for (int i = 0; i < files.length; i++) {
                        File file = files[i];

                        if (file.isDirectory()) {
                                deleteDir(file);
                        } else {
                                boolean deleted = file.delete();
                                if (!deleted) {
                                        throw new IOException("Unable to delete file" + file);
                                }
                        }
                }

                dir.delete();
        }

 public static void main(String[] args) throws Exception {
  FileSystem fs;
  Configuration conf;
  Job job;
    // variable to keep track of the recursion depth
  int depth = 0;

  Path in,out;
  System.out.println();
  System.out.println();
  System.out.println("Summary :: ");
  System.out.println("ITERATIONS   TIME(Millisecond)");
  System.out.println();
  System.out.println();
  
  int ITERATIONS = Integer.parseInt(args[1]);
    depth++;
    while (depth <= ITERATIONS) {
      long startTime=System.currentTimeMillis();

      // reuse the conf reference with a fresh object
      conf = new Configuration();
      // set the depth into the configuration
      conf.set("recursion.depth", depth + "");
      job = new Job(conf);
      job.setJobName("PageRank_" + depth);
 
      job.setMapperClass(PageRankMapper.class);
      job.setReducerClass(PageRankReducer.class);
      job.setJarByClass(PageRank.class);
      // always work on the path of the previous depth
      in = new Path(args[0]+"/depth_" + (depth - 1) + "/");
      out = new Path(args[0]+"/depth_" + depth);
      
      FileInputFormat.addInputPath(job, in);
      
      
 
      FileOutputFormat.setOutputPath(job, out);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      // wait for completion and update the counter
      job.waitForCompletion(true);
    // delete the outputpath if already exists
    if(depth > 1)
      deleteDir(new File(args[0]+"/depth_"+(depth-1)));
      
      long finishTime=System.currentTimeMillis();
      System.out.println(depth+"            "+(finishTime-startTime));
      System.out.println();   

      depth++;
    
  } 

      conf = new Configuration();
      // set the depth into the configuration
      job = new Job(conf);
      job.setJobName("Sorting");
 
      job.setMapperClass(PageRankSortMapper.class);
      job.setReducerClass(PageRankSortReducer.class);
      job.setJarByClass(PageRank.class);
      // always work on the path of the previous depth
      in = new Path(args[0]+"/depth_" + (depth - 1) + "/");
      out = new Path(args[0]+"/sorted");
 
      FileInputFormat.addInputPath(job, in);
      
      
 
      FileOutputFormat.setOutputPath(job, out);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      // wait for completion and update the counter
      job.waitForCompletion(true);

      System.out.println();
      System.out.println("Number of Nodes  : "+PageRankUtil.no_of_nodes);
      System.out.println();
      System.out.println("Number of Edges  : "+PageRankUtil.no_of_edges);
      System.out.println();
      System.out.println("Minimum number of outlinks : "+PageRankUtil.min_outlinks);
      System.out.println();
      System.out.println("Maximum number of outlinks : "+PageRankUtil.max_outlinks);
      System.out.println();
      System.out.println("Average number of outlinks : "+(PageRankUtil.no_of_nodes/PageRankUtil.no_of_edges));
      System.out.println();
      System.out.println();

 }
        
}
