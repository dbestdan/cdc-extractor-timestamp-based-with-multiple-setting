import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class CoordinatorRunnable implements Runnable, Config {
	private Connection conn = null;
	private PreparedStatement stmt = null;
	private ResultSet rs = null;
	private BlockingQueue<Task> queue = null;
	private long sessionEndTime = 0L;
	public static long sessionStartTime = 0L;
	private String tableName = null;
	private long endTime = 0L;
	private long tempEndTime = 0L;
	private Properties prop = null;

	public CoordinatorRunnable(String tableName, Properties prop, long sessionEndTime) {
		this.prop = prop;
		this.queue = new ArrayBlockingQueue<Task>(10000);
		;
		this.sessionEndTime = sessionEndTime;
		try {
			this.tableName = tableName;

			conn = Client.getConnection();

			/**
			 * 
			 * first we get get the current snapshot select
			 * txid_current_snapshot();
			 * 
			 * then we are getting the minimum transaction id that is still
			 * active and subtract that with 1, to get the transaction id which
			 * is just greater than the minimum active transaction id select
			 * txid_snapshot_xmin(txid_current_snapshot())-1;
			 * 
			 * then we get commit time stamp of that transaction id select
			 * pg_xact_commit_timestamp
			 * ((txid_snapshot_xmin(txid_current_snapshot())-1)::text::xid);
			 * 
			 * then we convert timestamp to miliscond select extract (epoch from
			 * pg_xact_commit_timestamp
			 * ((txid_snapshot_xmin(txid_current_snapshot())-1)::text::xid))
			 * *1000;
			 * 
			 **/

			String query = "select extract (epoch from pg_xact_commit_timestamp "
					+ "((txid_snapshot_xmin(txid_current_snapshot())-1)::text::xid)) *1000";
			stmt = conn.prepareStatement(query);

			sessionStartTime = System.currentTimeMillis();

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void run() {

		// read number of set and number of experiments from property file
		int noOfSet = Integer.parseInt(prop.getProperty("no_of_set"));
		int numberOfExperiment = Integer.parseInt(prop.getProperty("no_of_experiment"));

		// foe each set run number of experiments
		for (int i = 0; i < noOfSet; i++) {
			//each experiment manage sleeping time and run duration
			for (int j = 1; j <= numberOfExperiment; j++) {
				String experiment = prop.getProperty("experiment_" + j);
				String experimentParameter[] = experiment.split("_");
				long coordinatorSleepDuration = Long.parseLong(experimentParameter[1]);
				long runDuration = Long.parseLong(experimentParameter[3]) * 60000L;
				long experimentEndTime = System.currentTimeMillis() + runDuration;
				
				// for each experiment run duration
				while (experimentEndTime > System.currentTimeMillis()) {
					try {
						Thread.sleep(coordinatorSleepDuration);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					try {

						rs = stmt.executeQuery();
						rs.next();
						endTime = rs.getLong(1);
						if (tempEndTime < endTime) {
							queue.put(new Task(tempEndTime, endTime));
							tempEndTime = endTime;
						}

					} catch (SQLException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					} finally {
						try {
							rs.close();
							// stmt.close();
							// conn.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

				}
			}
		}
	}

	public BlockingQueue<Task> getQueue() {
		return queue;
	}

	public void setQueue(BlockingQueue<Task> queue) {
		this.queue = queue;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

}
