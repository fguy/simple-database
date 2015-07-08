package com.fguy;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringReader;

import org.junit.Assert;
import org.junit.Test;

/**
 * This class is to run test cases on the challenge requirement document.
 * 
 * @see http://www.thumbtack.com/challenges/simple-database
 * @author flowerguy@gmail.com
 *
 */
public class SimpleDatabaseTest {
	@Test
	public void simple() throws Throwable {
		StringBuilder sb = new StringBuilder();
		sb.append("SET ex 10\n");
		sb.append("GET ex\n");
		sb.append("UNSET ex\n");
		sb.append("GET ex\n");
		sb.append("END\n");
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		SimpleDatabase.watchCommands(new StringReader(sb.toString()),
				new PrintStream(baos));
		Assert.assertEquals("10\nNULL\n", baos.toString());
	}

	@Test
	public void numEqualTo() throws Throwable {
		StringBuilder sb = new StringBuilder();
		sb.append("SET a 10\n");
		sb.append("SET b 10\n");
		sb.append("NUMEQUALTO 10\n");
		sb.append("NUMEQUALTO 20\n");
		sb.append("SET b 30\n");
		sb.append("NUMEQUALTO 10\n");
		sb.append("END\n");
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		SimpleDatabase.watchCommands(new StringReader(sb.toString()),
				new PrintStream(baos));
		Assert.assertEquals("2\n0\n1\n", baos.toString());
	}

	@Test
	public void transaction() throws Throwable {
		StringBuilder sb = new StringBuilder();
		sb.append("BEGIN\n");
		sb.append("SET a 10\n");
		sb.append("GET a\n");
		sb.append("BEGIN\n");
		sb.append("SET a 20\n");
		sb.append("GET a\n");
		sb.append("ROLLBACK\n");
		sb.append("GET a\n");
		sb.append("ROLLBACK\n");
		sb.append("GET a\n");
		sb.append("END\n");
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		SimpleDatabase.watchCommands(new StringReader(sb.toString()),
				new PrintStream(baos));
		Assert.assertEquals("10\n20\n10\nNULL\n", baos.toString());
	}

	@Test
	public void noTransaction() throws Throwable {
		StringBuilder sb = new StringBuilder();
		sb.append("BEGIN\n");
		sb.append("SET a 30\n");
		sb.append("BEGIN\n");
		sb.append("SET a 40\n");
		sb.append("COMMIT\n");
		sb.append("GET a\n");
		sb.append("ROLLBACK\n");
		sb.append("END\n");
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		SimpleDatabase.watchCommands(new StringReader(sb.toString()),
				new PrintStream(baos));
		Assert.assertEquals("40\nNO TRANSACTION\n", baos.toString());
	}

	@Test
	public void transactionUnsetRollback() throws Throwable {
		StringBuilder sb = new StringBuilder();
		sb.append("SET a 50\n");
		sb.append("BEGIN\n");
		sb.append("GET a\n");
		sb.append("SET a 60\n");
		sb.append("BEGIN\n");
		sb.append("UNSET a\n");
		sb.append("GET a\n");
		sb.append("ROLLBACK\n");
		sb.append("GET a\n");
		sb.append("COMMIT\n");
		sb.append("GET a\n");
		sb.append("END\n");
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		SimpleDatabase.watchCommands(new StringReader(sb.toString()),
				new PrintStream(baos));
		Assert.assertEquals("50\nNULL\n60\n60\n", baos.toString());
	}

	@Test
	public void transactionNumEqualTo() throws Throwable {
		StringBuilder sb = new StringBuilder();
		sb.append("SET a 10\n");
		sb.append("BEGIN\n");
		sb.append("NUMEQUALTO 10\n");
		sb.append("BEGIN\n");
		sb.append("UNSET a\n");
		sb.append("NUMEQUALTO 10\n");
		sb.append("ROLLBACK\n");
		sb.append("NUMEQUALTO 10\n");
		sb.append("COMMIT\n");
		sb.append("END\n");
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		SimpleDatabase.watchCommands(new StringReader(sb.toString()),
				new PrintStream(baos));
		Assert.assertEquals("1\n0\n1\n", baos.toString());
	}
}
