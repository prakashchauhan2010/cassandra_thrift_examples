package reading_data;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Reading data from cassandra using simple get operation. Retrieves columns or
 * super columns using a column path.
 * 
 * @author Prakash Chauhan.
 */
public class UsingSimpleGet {
	private static final String HOST = "localhost";
	private static final int PORT = 9160;
	private static final String KEYSPACE = "prakash";
	private static final String COLUMN_FAMILY = "user";

	/**
	 * Connect to cassandra and read column using simple get.
	 * 
	 * @throws InvalidRequestException
	 * @throws TException
	 * @throws UnsupportedEncodingException
	 */
	public static void main(String... args) throws InvalidRequestException, TException,
			UnsupportedEncodingException {
		TTransport transport = new TSocket(HOST, PORT);
		TFramedTransport fTransport = new TFramedTransport(transport);
		TProtocol protocol = new TBinaryProtocol(fTransport);
		Cassandra.Client client = new Cassandra.Client(protocol);
		transport.open();

		// Connect to Cassandra at the specified keyspace.
		client.set_keyspace(KEYSPACE);

		/* Reading column using simple get */
		ColumnPath colPath = new ColumnPath(COLUMN_FAMILY);
		// Column to read.
		colPath.column = toByteBuffer("name");
		ColumnOrSuperColumn colOrSuperColumn = client.get(toByteBuffer("1"),colPath, ConsistencyLevel.ONE);
		Column column = colOrSuperColumn.column;
		System.out.println(toString(column.name) + " : " + toString(column.value));
	}

	private static ByteBuffer toByteBuffer(String value)
			throws UnsupportedEncodingException {
		return ByteBuffer.wrap(value.getBytes("UTF-8"));
	}

	public static String toString(ByteBuffer buffer)
			throws UnsupportedEncodingException {
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		return new String(bytes, "UTF-8");
	}
}
