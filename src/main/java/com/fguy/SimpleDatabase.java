package com.fguy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

/**
 * Simple Database Challenge
 * 
 * @see https://www.thumbtack.com/challenges/simple-database
 * @author flowerguy@gmail.com
 *
 */
public class SimpleDatabase {
	private final Map<String, Integer> _db = new HashMap<>();
	private final Map<Integer, Integer> _index = new HashMap<>();
	private final Stack<Map<String, Integer>> _transactions = new Stack<>();
	private final Stack<Map<Integer, Integer>> _transactionIndices = new Stack<>();
	private boolean _inTransaction = false;
	private PrintStream _out;

	SimpleDatabase(PrintStream out) {
		this._out = out;
	}

	/**
	 * Set the variable <code>name</code> to the value <code>value</code>.
	 * Neither variable names nor values will contain spaces.
	 * 
	 * @param name
	 * @param value
	 */
	void set(String name, int value) {
		Map<String, Integer> db = (_inTransaction ? _transactions.peek() : _db);
		if (db.containsKey(name)) { // remove from index if the name already
									// exists
			decreaseIndex(db.get(name));
		}
		db.put(name, value);
		increaseIndex(value);
	}

	private void increaseIndex(int value) {
		Map<Integer, Integer> index = _inTransaction ? _transactionIndices
				.peek() : _index;
		index.put(value, index.containsKey(value) ? index.get(value) + 1 : 1);
	}

	/**
	 * Returns the value of the variable <code>name</code>, or null if that
	 * variable is not set.
	 * 
	 * @param name
	 * @return
	 */
	Integer get(String name) {
		for (int i = _transactions.size() - 1; i > -1; i--) {
			Map<String, Integer> transaction = _transactions.get(i);
			if (transaction.containsKey(name)) {
				return transaction.get(name); // returns most recent from
												// transactions
			}
		}
		// otherwise, return directly from db.
		return _db.get(name);
	}

	/**
	 * Unset the variable <code>name</code>, making it just like that variable
	 * was never set.
	 * 
	 * @param name
	 */
	void unset(String name) {
		Integer value;
		if (_inTransaction) {
			Map<String, Integer> db = _transactions.peek();
			value = get(name);
			db.put(name, null); // mark as null
		} else {
			value = _db.remove(name);
		}

		if (value == null) {
			return;
		}

		decreaseIndex(value);
	}

	private void decreaseIndex(int value) {
		Map<Integer, Integer> index = _inTransaction ? _transactionIndices
				.peek() : _index;
		index.put(value, index.containsKey(value) ? index.get(value) - 1 : -1);
	}

	/**
	 * Returns the number of variables that are currently set to
	 * <code>value</code>. If no variables equal that value, returns 0.
	 * 
	 * @param value
	 * @return
	 */
	int numEqualTo(int value) {
		int result = _index.containsKey(value) ? _index.get(value) : 0;
		// accumulate indices from transactions.
		for (Map<Integer, Integer> index : _transactionIndices) {
			if (index.containsKey(value)) {
				result += index.get(value);
			}
		}
		return result;
	}

	/**
	 * Open a new transaction block. Transaction blocks can be nested; a BEGIN
	 * can be issued inside of an existing block.
	 */
	void begin() {
		_inTransaction = true;
		_transactions.push(new HashMap<>());
		_transactionIndices.push(new HashMap<>());
	}

	/**
	 * Undo all of the commands issued in the most recent transaction block, and
	 * close the block. Print nothing if successful, or print NO TRANSACTION if
	 * no transaction is in progress.
	 */
	void rollback() {
		if (_transactions.empty()) {
			_out.println("NO TRANSACTION");
			return;
		}
		Map<String, Integer> db = _transactions.pop();
		_transactionIndices.pop();
		if (db.size() == 0) {
			_out.println("NO TRANSACTION");
			return;
		}
		boolean isNoTransaction = true;
		for (String name : db.keySet()) { // check whether it has any change.
			if (db.containsKey(name)) {
				isNoTransaction = false;
				break;
			}
		}
		if (isNoTransaction) {
			_out.println("NO TRANSACTION");
			return;
		}
		_inTransaction = !_transactions.empty(); // set true if there's nested
													// transaction.
	}

	/**
	 * Close all open transaction blocks, permanently applying the changes made
	 * in them. Print nothing if successful, or print NO TRANSACTION if no
	 * transaction is in progress.
	 */
	void commit() {
		if (!_inTransaction || _transactions.empty()) {
			return;
		}
		Map<String, Integer> db = _transactions.pop();
		_inTransaction = false;
		Set<String> committed = new HashSet<>();
		for (String name : db.keySet()) {
			set(name, db.get(name));
			committed.add(name);
		}
		removeCommitted(committed);

		_inTransaction = !_transactions.empty();
		_transactionIndices.pop();
	}

	/**
	 * Removes committed variables from outer transactions after a commit.
	 * 
	 * @param committed
	 */
	private void removeCommitted(Set<String> committed) {
		for (Map<String, Integer> transaction : _transactions) {
			Set<String> names = transaction.keySet();
			names.retainAll(committed);
			for (String name : names) {
				transaction.remove(name);
			}
		}
	}

	public static void main(String args[]) throws IOException {
		watchCommands(new InputStreamReader(System.in), System.out);
	}

	/**
	 * Parse commands and call corresponding methods.
	 * 
	 * @param r
	 * @param out
	 * @throws IOException
	 */
	static void watchCommands(Reader r, PrintStream out) throws IOException {
		SimpleDatabase instance = new SimpleDatabase(out);
		BufferedReader br = new BufferedReader(r);
		String line;
		while (!"END".equals(line = br.readLine().trim())) { // Exit the
																// program. Your
																// program will
																// always
																// receive this
																// as its last
																// command.
			String[] tokens = line.split("\\s+");
			Integer result = null;
			switch (tokens[0]) {
			case "SET":
				instance.set(tokens[1], Integer.parseInt(tokens[2]));
				break;
			case "GET":
				result = instance.get(tokens[1]);
				out.println(result == null ? "NULL" : result);
				break;
			case "UNSET":
				instance.unset(tokens[1]);
				break;
			case "NUMEQUALTO":
				out.println(instance.numEqualTo(Integer.parseInt(tokens[1])));
				break;
			case "BEGIN":
				instance.begin();
				break;
			case "ROLLBACK":
				instance.rollback();
				break;
			case "COMMIT":
				instance.commit();
				break;
			default:
				System.err.println("Invalid command.");
			}
		}
	}
}
