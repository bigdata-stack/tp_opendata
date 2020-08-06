package m2.miage.bigdata.tp.opendata.part2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilesEquipementsReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

	public void reduce(IntWritable key, Iterable<Text> values, Context sortie)
			throws IOException, InterruptedException {

		Iterator<Text> it = values.iterator();
		Configuration conf = sortie.getConfiguration();
		String getNameFile1 = conf.get("nomFichier1");
		String getNameFile2 = conf.get("nomFichier2");
		String separateur = conf.get("separateur");
		int orderbyFile = Integer.valueOf(conf.get("ordre"));

		String resultEquipement = "";
		String resultEquipActivitis = "";
		while (it.hasNext()) {
			String[] listValues = it.next().toString().split(separateur);

			if (listValues[0].equals(getNameFile1)) {
				for (int i = 1; i < listValues.length; i++) {
					resultEquipement += listValues[i] + separateur;
				}
			} else if (listValues[0].equals(getNameFile2)) {
				for (int i = 1; i < listValues.length; i++) {
					resultEquipActivitis += listValues[i] + separateur;
				}
			}
		}
		if (orderbyFile == 1) {
			for (String item : resultEquipActivitis.split(separateur)) {
				sortie.write(new IntWritable(key.get()),
						new Text(resultEquipement.substring(0, resultEquipement.length() - 1) + separateur + item));
			}

		} else {
			for (String item : resultEquipement.split(separateur)) {
				
				sortie.write(new IntWritable(key.get()), new Text(
						resultEquipActivitis.substring(0, resultEquipActivitis.length() - 1) + separateur + item));
			}
		}

	}
}