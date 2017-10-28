import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class WorkerRunnable implements Runnable, Config {
	private BlockingQueue<Task> queue = null;

	private Writer out = null;


	private String threadName = "";
	private String tableInitial = "";
	private String tableName = "";
	//private long sessionNextTimeStamp = 0;
	private long sessionEndTime = 0L;

	private CoordinatorRunnable parent = null;

	private ResultSetMetaData rsmd = null;
	private int columnCount = 0;
	private Object lock = null;
	

	public WorkerRunnable(String threadName, String tableName, String tableInitial, 
						CoordinatorRunnable parent,
						long sessionEndTime, Object lock) {
		this.threadName = threadName;
		this.tableName = tableName;
		this.tableInitial = tableInitial;
		this.parent = parent;
		this.queue = parent.getQueue();
		this.sessionEndTime = sessionEndTime;

		String fileName = "chunk_" + threadName;
		try {
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName, true), "UTF-8"));
		} catch (UnsupportedEncodingException | FileNotFoundException e) {
			e.printStackTrace();
		}
		this.lock = lock;

	}

	@Override
	public void run() {

		Connection conn = Client.getConnection();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			String query = "select * from " + tableName + " where  " + tableInitial + "_modified_time >= ? and "
					+ tableInitial + "_modified_time < ? ";

			stmt = conn.prepareStatement(query);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		try {

			while (sessionEndTime > System.currentTimeMillis()) {

				Task task = queue.take();



				stmt.setLong(1, task.getStartTime());
				stmt.setLong(2, task.getEndTime());
				rs = stmt.executeQuery();
				rsmd= rs.getMetaData();
				columnCount = rsmd.getColumnCount();

				while (rs.next()) {

					// write to a file
					writeLocalFile(rs);

				}

				long tmp = task.getEndTime();
				// if commit timestamp is latest then update fresjmess time
				if (Client.freshness < tmp) {
					Client.freshness = tmp;
					synchronized (lock) {
						lock.notifyAll();
					}
				}

			}
			System.out.println("thread exit: " + threadName);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
				out.close();
			} catch (SQLException | IOException e) {
				e.printStackTrace();
			}
		}

	}

	private void writeLocalFile(ResultSet rs) {

		try {
			StringBuffer sb = new StringBuffer();
			sb.append(tableName + "|");
			for (int i = 1; i < columnCount; i++) {
				sb.append(rs.getString(i) + "|");
			}
			sb.append("\n");
			out.append(sb.toString());
			out.flush();
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}

	}

}
