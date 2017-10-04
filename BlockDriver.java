import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import java.util.LinkedList;

public class BlockDriver {

	public static enum PRCounters {
		residuals,
		numOfIter
	};
	
	/**
	 * createa a MapReduce job
	 * @param jobId: number of job
	 * @param inputDirectory: name of input folder
	 * @param outputDirectory: name of output folder
	 * @return MapReduce job
	 * @throws IOException
	 */
	public static Job createUpdateJob (int jobId, String inputDirectory, String outputDirectory) 
		throws IOException 
	{
		String jID = (new Integer(jobId)).toString();
		Job job = new Job(new Configuration(), "PageRank_" + jID);
		job.setJarByClass(BlockDriver.class);
		job.setMapperClass(BlockMapper.class);
		job.setReducerClass(BlockReducerJacobi.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputDirectory));
		FileOutputFormat.setOutputPath(job, new Path(outputDirectory + "/" + "PageRank_" + jID));
		return job;
	}

	/**
	 * run blocked rankpage for one time
	 * @param jobId: number of job
	 * @param inputDirectory: name of input folder
	 * @param outputDirectory: name of output folder
	 * @return average residuals for all nodes
	 * @throws Exception
	 */
	public static double runBlockedRP (int jobId, String inputDirectory, String outputDirectory) 
		throws Exception 
	{
		Job j = createUpdateJob(jobId, inputDirectory, outputDirectory);
		j.waitForCompletion(true);

		Counters cnt = j.getCounters();
		long totalResiduals = cnt.findCounter(PRCounters.residuals).getValue();
		long totalIter = cnt.findCounter(PRCounters.numOfIter).getValue();
		double avgResiduals = (double)totalResiduals / Constant.NUM_ALL_NODE / Constant.DECIMAL_ACCURACY;
		double avgIter = (double)totalIter / Constant.BLOCKNUM;
		System.out.println("Iteration " + jobId + " avg error " + avgResiduals);
		System.out.println("Average inner loop for iteration " + jobId + ": " + avgIter);
		if (Constant.SHOW_PAGERANK) {
			System.out.println("***************************************************************************");
		}
		System.out.println();
		cnt.findCounter(PRCounters.residuals).setValue((long)0);
		cnt.findCounter(PRCounters.numOfIter).setValue((long)0);
		return avgResiduals;
	}

	public static void main (String[] args) throws Exception {

		// Constant.setInBlockNum();	// initialize number of nodes in block
		int jID = 0;
		String inputDirectory = args[0];
		String outputDirectory = args[1];
		String isShowPageRank = args[2];

		if (isShowPageRank.toLowerCase().trim().equals("true")) {
			Constant.SHOW_PAGERANK = true;
		} else {
			Constant.SHOW_PAGERANK = false;
		}
		System.out.println("Constant.SHOW_PAGERANK: " + Constant.SHOW_PAGERANK);
		System.out.println();
		double curAvgResiduals = runBlockedRP(jID, inputDirectory, outputDirectory);

		int round = 1;
		while (curAvgResiduals > Constant.EPSILON) {
		//for (int i = 0; i < round; i ++) {
			inputDirectory = outputDirectory + "/" + "PageRank_" + jID;	// the last output path
			jID += 1;
			curAvgResiduals = runBlockedRP(jID, inputDirectory, outputDirectory);	
		}
	}
}