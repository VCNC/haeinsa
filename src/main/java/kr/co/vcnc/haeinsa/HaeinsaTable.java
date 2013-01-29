package kr.co.vcnc.haeinsa;

import java.io.IOException;
import java.util.List;

import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public interface HaeinsaTable {
	interface PrivateIface extends HaeinsaTable {
		void prewrite(RowTransactionState rowTxState, byte[] row, boolean isPrimary) throws IOException;
		
		void applyDeletes(RowTransactionState rowTxState, byte[] row) throws IOException;
		/**
		 * make row from {@link RowState#PREWRITTEN} or {@link RowState#COMMITTED} or {@link RowState#ABORTED} to {@link RowState#STABLE}
		 * @param tx
		 * @param row
		 * @throws IOException
		 */
		void makeStable(RowTransactionState rowTxState, byte[] row) throws IOException;
		
		/**
		 * make primary row from {@link RowState#PREWRITTEN} to {@link RowState#COMMITTED}
		 * @param tx
		 * @param row
		 * @throws IOException
		 */
		void commitPrimary(RowTransactionState rowTxState, byte[] row) throws IOException;
			
		/**
		 * get {@link TRowLock}
		 * @param row row
		 * @return row lock
		 * @throws IOException
		 */
		TRowLock getRowLock(byte[] row) throws IOException;
		
		/**
		 * make primary row from {@link RowState#PREWRITTEN} to {@link RowState#ABORTED}  
		 * @param tx
		 * @param row
		 * @throws IOException
		 */
		void abortPrimary(RowTransactionState rowTxState, byte[] row) throws IOException;
		
		/**
		 * delete primary row's puts({@link TRowLock#puts}).
		 * @param rowTxState
		 * @param row
		 * @throws IOException
		 */
		void deletePuts(RowTransactionState rowTxState, byte[] row) throws IOException;
	}

	  /**
	   * Gets the name of this table.
	   *
	   * @return the table name.
	   */
	  byte[] getTableName();

	  /**
	   * Returns the {@link Configuration} object used by this instance.
	   * <p>
	   * The reference returned is not a copy, so any change made to it will
	   * affect this instance.
	   */
	  Configuration getConfiguration();

	  /**
	   * Gets the {@link HTableDescriptor table descriptor} for this table.
	   * @throws IOException if a remote or network exception occurs.
	   */
	  HTableDescriptor getTableDescriptor() throws IOException;
	  
	  /**
	   * Extracts certain cells from a given row.
	   * @param get The object that specifies what data to fetch and from which row.
	   * @return The data coming from the specified row, if it exists.  If the row
	   * specified doesn't exist, the {@link Result} instance returned won't
	   * contain any {@link KeyValue}, as indicated by {@link Result#isEmpty()}.
	   * @throws IOException if a remote or network exception occurs.
	   * @since 0.20.0
	   */
	  Result get(Transaction tx, HaeinsaGet get) throws IOException;

	  /**
	   * Extracts certain cells from the given rows, in batch.
	   *
	   * @param gets The objects that specify what data to fetch and from which rows.
	   *
	   * @return The data coming from the specified rows, if it exists.  If the row
	   *         specified doesn't exist, the {@link Result} instance returned won't
	   *         contain any {@link KeyValue}, as indicated by {@link Result#isEmpty()}.
	   *         If there are any failures even after retries, there will be a null in
	   *         the results array for those Gets, AND an exception will be thrown.
	   * @throws IOException if a remote or network exception occurs.
	   *
	   * @since 0.90.0
	   */
	  Result[] get(Transaction tx, List<HaeinsaGet> gets) throws IOException;

	  /**
	   * Returns a scanner on the current table as specified by the {@link Scan}
	   * object.
	   *
	   * @param scan A configured {@link Scan} object.
	   * @return A scanner.
	   * @throws IOException if a remote or network exception occurs.
	   * @since 0.20.0
	   */
	  ResultScanner getScanner(Transaction tx, Scan scan) throws IOException;

	  /**
	   * Gets a scanner on the current table for the given family.
	   *
	   * @param family The column family to scan.
	   * @return A scanner.
	   * @throws IOException if a remote or network exception occurs.
	   * @since 0.20.0
	   */
	  ResultScanner getScanner(Transaction tx, byte[] family) throws IOException;

	  /**
	   * Gets a scanner on the current table for the given family and qualifier.
	   *
	   * @param family The column family to scan.
	   * @param qualifier The column qualifier to scan.
	   * @return A scanner.
	   * @throws IOException if a remote or network exception occurs.
	   * @since 0.20.0
	   */
	  ResultScanner getScanner(Transaction tx, byte[] family, byte[] qualifier) throws IOException;


	  /**
	   * Puts some data in the table.
	   * <p>
	   * If {@link #isAutoFlush isAutoFlush} is false, the update is buffered
	   * until the internal buffer is full.
	   * @param put The data to put.
	   * @throws IOException if a remote or network exception occurs.
	   * @since 0.20.0
	   */
	  void put(Transaction tx, HaeinsaPut put) throws IOException;

	  /**
	   * Puts some data in the table, in batch.
	   * <p>
	   * If {@link #isAutoFlush isAutoFlush} is false, the update is buffered
	   * until the internal buffer is full.
	   * <p>
	   * This can be used for group commit, or for submitting user defined
	   * batches.  The writeBuffer will be periodically inspected while the List
	   * is processed, so depending on the List size the writeBuffer may flush
	   * not at all, or more than once.
	   * @param puts The list of mutations to apply. The batch put is done by
	   * aggregating the iteration of the Puts over the write buffer
	   * at the client-side for a single RPC call.
	   * @throws IOException if a remote or network exception occurs.
	   * @since 0.20.0
	   */
	  void put(Transaction tx, List<HaeinsaPut> puts) throws IOException;

	  /**
	   * Deletes the specified cells/row.
	   *
	   * @param delete The object that specifies what to delete.
	   * @throws IOException if a remote or network exception occurs.
	   * @since 0.20.0
	   */
	  void delete(Transaction tx, HaeinsaDelete delete) throws IOException;

	  /**
	   * Deletes the specified cells/rows in bulk.
	   * @param deletes List of things to delete.  List gets modified by this
	   * method (in particular it gets re-ordered, so the order in which the elements
	   * are inserted in the list gives no guarantee as to the order in which the
	   * {@link Delete}s are executed).
	   * @throws IOException if a remote or network exception occurs. In that case
	   * the {@code deletes} argument will contain the {@link Delete} instances
	   * that have not be successfully applied.
	   * @since 0.20.1
	   */
	  void delete(Transaction tx, List<HaeinsaDelete> deletes) throws IOException;

	  /**
	   * Releases any resources help or pending changes in internal buffers.
	   *
	   * @throws IOException if a remote or network exception occurs.
	   */
	  void close() throws IOException;
}
