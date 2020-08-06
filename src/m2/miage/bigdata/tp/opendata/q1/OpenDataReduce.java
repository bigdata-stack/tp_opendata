package m2.miage.bigdata.tp.opendata.q1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OpenDataReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Context sortie)
			throws IOException, InterruptedException {
		Iterator<IntWritable> it = values.iterator();
		int nombreElement = 0;
		while (it.hasNext()) {
			nombreElement += it.next().get();
		}
		sortie.write(key, new IntWritable(nombreElement));
	}
}
