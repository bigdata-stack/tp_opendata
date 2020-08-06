package m2.miage.bigdata.tp.opendata.q1;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OpenDataMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> 
{
	public void map(LongWritable cle, Text valeur, Context sortie)
	throws IOException 
	{
		try 
		{	
			String[] line = valeur.toString().split(";");	
			if(cle.get()!=0 && !line[1].equalsIgnoreCase("DepLib")&& !line[6].equalsIgnoreCase("EquNom")&& !line[9].equalsIgnoreCase("EquipementTypeLib")) {	
				sortie.write(new Text(line[1]+" - "+line[9]), new IntWritable(1));
			}
		} 
		catch (Exception e) 
		{
			throw new RuntimeException(e.getMessage());
		}
	}
}

