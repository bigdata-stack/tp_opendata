package m2.miage.bigdata.tp.opendata.q4;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class OpenDataMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	public void map(LongWritable cle, Text valeur, Context sortie) 	throws IOException 
	{
		try 
		{	
			String[] line = valeur.toString().split(";");	
			if(cle.get()!=0 && line[1].contains("Haute-Garonne")) {
				double dist = 0;
				if (!line[196].isEmpty() && !line[197].isEmpty()) 
				{
					if(Math.abs(Double.valueOf(1.450) - Double.  valueOf(line[196])) > 10){
						String temp = line[196];
						line[196] =line[197]; 
						line[197]=temp;
					}
					CalculDistance cal_dist= new  CalculDistance();
					dist=cal_dist.distance(Double.valueOf(1.450),Double.valueOf(43.617), Double.valueOf(line[197]), Double.valueOf(line[196]));
				}
				sortie.write(new Text(line[1]), new Text(dist+" "+1+" "));
			}
		} 
		catch (Exception e) 
		{
			throw new RuntimeException(e.getLocalizedMessage());
		}
	}
}

