
import java.io.BufferedReader;
import java.io.FileReader;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public class CostFromLog {
	public static void main(String [] args) throws Exception {
		if (args.length != 1) {
			System.out.println("Usage: CostFromLog <logfilename>");
			System.exit(-1);
		}

		int cents = 0;
		BufferedReader rdr = new BufferedReader(new FileReader(args[0]));
		HashMap<String, Date> startTimes = new HashMap<String, Date>();
		Date lastDate = null;
		SimpleDateFormat fmt = new SimpleDateFormat("MMM d, yyyy h:mm:ss a");

		String line = rdr.readLine();
		while (line != null) {
			Date tmp = fmt.parse(line, new ParsePosition(0));
			if (tmp != null) {
				lastDate = tmp;
			}
			else {	// look for other things
				if (line.contains("instance started")) {
					String id = line.substring(line.lastIndexOf(':')+1).trim();
					if (startTimes.containsKey(id)) {
						System.err.println("Start time already registered for :"+id);
					}
					startTimes.put(id, lastDate);
				}
				if (line.contains("instance terminated")) {
					String id = line.substring(line.lastIndexOf(':')+1).trim();
					Date start = startTimes.get(id);
					int secondsUp = (int)((lastDate.getTime() - start.getTime())/1000);
					int minutesUp = secondsUp / 60;
					int hoursUp = minutesUp / 60;
					int minutesRemain = minutesUp - (hoursUp * 60);
					int secondsRemain = secondsUp - (minutesUp * 60);
					System.out.println("instance "+id+" up :"+hoursUp+":"+minutesRemain+":"+secondsRemain);
					if (hoursUp < 0) {
						System.err.println("Error with dates! : start="+fmt.format(start)+" end="+fmt.format(lastDate));
					}
					else {
						cents += hoursUp * 10 + (((minutesRemain>0) || (secondsRemain>0))?10:0);
					}
					startTimes.remove(id);
				}
			}

			line = rdr.readLine();
		}
		System.out.println("cost of run : $"+cents/100.0);
	}
}
