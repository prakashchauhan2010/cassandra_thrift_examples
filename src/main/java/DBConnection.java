package main.java;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * @author Prakash Chauhan.
 */
public class DBConnection {
	private static final String HOST = "localhost";
	private static final int PORT = 9160;
	private static final String KEYSPACE = "prakash";
	private static final String COLUMN_FAMILY = "user";
	private Cassandra.Client client;

	/**
	 * Created database connection.
	 * 
	 * @throws InvalidRequestException
	 * @throws TException
	 */
	public void createDatabaseConnection() throws InvalidRequestException,
			TException {
		TTransport transport = new TSocket(HOST, PORT);
		TFramedTransport fTransport = new TFramedTransport(transport);
		TProtocol protocol = new TBinaryProtocol(fTransport);
		client = new Cassandra.Client(protocol);
		transport.open();
		
		// Connect to Cassandra at the specified keyspace.
	    client.set_keyspace(KEYSPACE);
	}

	/**
	 * Add a new column for the specified row key.
	 * 
	 * @param rowId
	 * @param colName
	 * @param colValue
	 * @throws UnsupportedEncodingException
	 * @throws TException
	 * @throws TimedOutException
	 * @throws UnavailableException
	 * @throws InvalidRequestException
	 */
	public void insertColumn(String rowId, String colName, String colValue)
			throws UnsupportedEncodingException, InvalidRequestException,
			UnavailableException, TimedOutException, TException {
		ColumnParent cParent = new ColumnParent(COLUMN_FAMILY);
		Column column = new Column(toByteBuffer(colName));
		column.setValue(colValue.getBytes());
		// Timestamp tells when this insert was perfomed.
		column.setTimestamp(System.currentTimeMillis());
		
		client.insert(toByteBuffer(rowId), cParent, column,ConsistencyLevel.ONE);
	}

	private static ByteBuffer toByteBuffer(String value)
			throws UnsupportedEncodingException {
		return ByteBuffer.wrap(value.getBytes("UTF-8"));
	}
	
	public static void main(String[] args) {
		DBConnection dbConnection = new DBConnection();
		try {
			dbConnection.createDatabaseConnection();
			dbConnection.insertColumn("1", "loc", "Gurgaon");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
