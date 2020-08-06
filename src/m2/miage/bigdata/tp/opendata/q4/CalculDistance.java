package m2.miage.bigdata.tp.opendata.q4;

public class CalculDistance {
	private final static double constante=Math.PI/180;
	public CalculDistance() {
		
	}	
	
	public double distance(double lat_pos1, double lon_pos1,double lat_pos2,double lon_pos2) {
		double lat1= lat_pos1*constante;
		double lon1=lon_pos1*constante;
		double lat2=lat_pos2*constante;
		double lon2=lon_pos2*constante;
		double t1=Math.sin(lat1)*Math.sin(lat2);
		double t2=Math.cos(lat1)*Math.cos(lat2);
		double t3=Math.cos(lon1-lon2);
		double t4=t2*t3;
		double t5=t1+t4;
		double rad_dist=Math.atan(-t5/Math.sqrt(-t5*t5+1))+2*Math.atan(1);
		return (rad_dist*3437.74677*1.1508)*1.6093470878864446; 
	}
	
	

}
