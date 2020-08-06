package m2.miage.bigdata.tp.opendata.part2.q3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OpenDataReduce extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context sortie)
			throws IOException, InterruptedException {
		Iterator<Text> it = values.iterator();
		String valeurs="";
		int currentCompte=0;
		while (it.hasNext()) {
			String[] line=it.next().toString().split(" ");			
			if(line.length>1) {
				valeurs += line[1]+" ¤ ";
			}
			currentCompte++;
		}
		sortie.write(new Text(key+" "+valeurs), new Text(" :"+currentCompte));
	}
}
