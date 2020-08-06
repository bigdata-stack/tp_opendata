package m2.miage.bigdata.tp.opendata.part2.q2;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class OpenDataMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	public void map(LongWritable cle, Text valeur, Context sortie) throws IOException 
	{
		try 
		{	
			String[] line = valeur.toString().split(";");
			String nomFichier=null;
			nomFichier =((FileSplit) sortie.getInputSplit()).getPath().getName();
			//nomFichier=nomFichier.toString().split("_")[2];					
			String keys="";
			String values="";
			if(nomFichier!=null && cle.get()!=0) {
				if(nomFichier.contains("FichesEquipementsActivites")) {
					if( !line[7].isEmpty() && !line[4].isEmpty()) {	
						keys=line[4]; //20000
						values=nomFichier+" "+line[7]; 
					}
				}else if (nomFichier.contains("FichesEquipements_small")) {
					if(!line[6].isEmpty()&& !line[3].isEmpty()&&!line[7].isEmpty()) {	
						keys=line[6]; 
						values=nomFichier+" "+line[3]+" "+line[7]; 
					}			
				}
				sortie.write(new Text(keys), new Text(values));
			}
		} 
		catch (Exception e) 
		{
			throw new RuntimeException(e.getMessage());
		}
	}
}

