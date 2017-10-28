import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.ErrorManager;

public class Client {
	public static long freshness = 0L;

	public static void main(String[] args) {
		freshness = System.currentTimeMillis();

		final Object lock = new Object();

		ArrayList<Thread> threads = new ArrayList<Thread>();

		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(System.getProperty("prop")));
		} catch (IOException e) {
			System.out.println("CDC-Extractor-Time-based: Could not load property file");
		}

		// run duration
		long runDuration = Long.parseLong(prop.getProperty("run_duration")) * 60000L;
		long sessionEndTime = System.currentTimeMillis() + runDuration;
		System.out.println("run Duration =" + runDuration);

		String clientName = System.getProperty("client");
		String clientTables = "";

		// start socket to communicate with cdc-requester
		SocketServerRunnable serverSocket = new SocketServerRunnable(lock);
		threads.add(new Thread(serverSocket));

		clientTables = prop.getProperty(clientName);

		String[] clientTablesArray = clientTables.split(",");

		for (int i = 0; i < clientTablesArray.length; i++) {
			String tableNameAndThreadSize = clientTablesArray[i];
			System.out.println("table name and thread size "+tableNameAndThreadSize);
			String[] tableNameAndThreadSizeArray = tableNameAndThreadSize.split("-");
			String tableName = tableNameAndThreadSizeArray[0];
			String tableInitial = tableNameAndThreadSizeArray[1];
			int threadSize = Integer.parseInt(tableNameAndThreadSizeArray[2]);

			// one coordinator for each table but worker thread may be more than
			// one
			CoordinatorRunnable coordinator = new CoordinatorRunnable(tableName,prop, sessionEndTime);
			threads.add(new Thread(coordinator));
			// for worker thread of each table
			for (int j = 0; j < threadSize; j++) {
				WorkerRunnable worker = new WorkerRunnable(tableName + j, tableName, tableInitial, coordinator,
						sessionEndTime, lock);
				threads.add(new Thread(worker));
			}
		}

		QueryRequestRunnable queryRequestRunnable = new QueryRequestRunnable();
		threads.add(new Thread(queryRequestRunnable));

		for (int i = 0; i < threads.size(); i++) {
			threads.get(i).start();
		}
		try {
			Thread.sleep(runDuration);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.exit(0);

	}

	public static Connection getConnection() {
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(System.getProperty("prop")));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		String user = prop.getProperty("user");
		String password = prop.getProperty("password");
		String url = prop.getProperty("url");

		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", user);
		connectionProps.put("password", password);
		try {
			Class.forName("org.postgresql.Driver");
			conn = DriverManager.getConnection(url, connectionProps);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

}
