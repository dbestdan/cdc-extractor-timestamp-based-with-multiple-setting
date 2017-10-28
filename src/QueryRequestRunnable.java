import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Timestamp;
import java.util.Date;

public class QueryRequestRunnable implements Runnable, Config {
	private Writer out;
	private long sessionNextTimeStamp = 0L;
	private int count = 0;
	private long totalStaleness = 0L;
	private long avgStaleness = 0L;

	public QueryRequestRunnable() {
		String stalenessFileName = "staleness_" + System.getProperty("client") + "_coordinator_sleep_time_"
				+ System.getProperty("sleepDuration") + "_" + dateFormat.format(new Date());

		try {
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(stalenessFileName, true), "UTF-8"));

		} catch (UnsupportedEncodingException | FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		sessionNextTimeStamp = System.currentTimeMillis() + 60000;
		while (true) {
			if (Client.freshness > 0) {
				try {


					long staleness = System.currentTimeMillis() - Client.freshness;
					totalStaleness += staleness;
					count++;
					avgStaleness = totalStaleness / count;


					out.append((System.currentTimeMillis() - CoordinatorRunnable.sessionStartTime) + "," + avgStaleness
							+ "\n");
						out.flush();

				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			// have a file to record staleness
			// buffer the staleness into a list, flush the list periodically to
			// the on-disk file
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
