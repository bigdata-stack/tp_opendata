package m2.miage.bigdata.tp.opendata.part2.q2;

import java.io.IOException;
import java.util.Iterator;

import javax.swing.JSpinner.ListEditor;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OpenDataReduce extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context sortie)
			throws IOException, InterruptedException {
		Iterator<Text> it = values.iterator();
		String resultEquipenent = "";
		String resultEquiActivites = "";
		int currentCompte=0;
		while (it.hasNext()) {
			String[] listValues = it.next().toString().split(" ");

			if (listValues[0].contains("FichesEquipementsActivites")){
				resultEquiActivites += listValues[2] +";";
			}
			
			if (listValues[0].contains("FichesEquipements_small")){
				resultEquipenent += "\t" +listValues[2] + "¤" +listValues[3];
			}
			currentCompte++;
		}
		for (String S: resultEquiActivites.split(";")){
			sortie.write(new Text(key + resultEquipenent + "¤" + S),new Text("\t"+ currentCompte));
		}
	}
}
