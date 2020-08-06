import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainFiles {

	public static void main(String[] args) throws Exception {

		Job job = new Job();
		
		Configuration conf = job.getConfiguration();

		conf.set("nomFichier1", args[2]);
		conf.set("colonneID1", args[3]);
		conf.set("listeDesColonnesProjetFile1", args[4]);
		conf.set("nomFichier2", args[5]);
		conf.set("colonneID2", args[6]);
		conf.set("listeDesColonnesProjetFile2", args[7]);
		conf.set("ordre", args[8]);
		conf.set("separateur", args[9]);

		job.setJobName("TP File Open Data");

		job.setJarByClass(MainFiles.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(m2.miage.bigdata.tp.opendata.part2.FilesEquipementsMapper.class);
		job.setReducerClass(m2.miage.bigdata.tp.opendata.part2.FilesEquipementsReducer.class);

		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileSystem fs = FileSystem.get(job.getConfiguration());
		fs.delete(new Path(args[1]));

		job.waitForCompletion(true);
	}
}
