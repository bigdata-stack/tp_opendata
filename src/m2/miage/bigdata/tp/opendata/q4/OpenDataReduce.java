package m2.miage.bigdata.tp.opendata.q4;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OpenDataReduce extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context sortie)
			throws IOException, InterruptedException {
		Iterator<Text> it = values.iterator();
		int currentCount=0;
		double moyen = 0;		
		while (it.hasNext()) {
			String line=it.next().toString();
			currentCount+=Double.valueOf(line.split(" ")[1]);
			moyen+=Double.valueOf(line.split(" ")[0]);
		}		
		sortie.write(new Text(key+" Nb d'équipements sportifs :"), new Text(" "+currentCount));
		sortie.write(new Text(key+" Distance Moyenne :"), new Text(" "+moyen/currentCount));

	}
}
