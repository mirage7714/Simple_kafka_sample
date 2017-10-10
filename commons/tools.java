package commons;

import java.text.SimpleDateFormat;
import java.util.Date;

public class tools {
	public static String T(){
		SimpleDateFormat f = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
		Date d = new Date();
		String t = f.format(d);
		return "["+t+"]";
	}
}
