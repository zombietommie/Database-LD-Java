/**
 * Josh Tran
 * Jenny Le
 * Tommy Tran
 */

package main;

import java.awt.print.Printable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.Scanner;

import oracle.jdbc.proxy.annotation.Pre;

import java.sql.Date;

public class QueryLD {

	Connection conn;
	final String host = "dbsvcs.cs.uno.edu";
	final int port = 1521;
	final String sID = "orcl";

	// Three database connection link constructors
	public QueryLD(String host, int port, String sID, String username, String passwd) throws SQLException {
		conn = new DatabaseConnection(host, port, sID).getDatabaseConnection(username, passwd);
	}

	public QueryLD(String username, String passwd) throws SQLException {
		this.conn = new DatabaseConnection(host, port, sID).getDatabaseConnection(username, passwd);
	}

	public QueryLD(Connection conn) throws SQLException {
		this.conn = conn;
	}
	// END OF connection constructors

	/**
	 * Query 13 setup
	 * 
	 * @param per_id
	 * @return
	 * @throws SQLException
	 */
	public ArrayList<String[]> queryThirteen(String per_id) throws SQLException {
		String str = "SELECT first_name, last_name, job_title " + "FROM Person NATURAL JOIN works "
				+ "WHERE per_id = ? ";
		ArrayList<String[]> al = new ArrayList<String[]>();
		PreparedStatement pStmt = conn.prepareStatement(str);
		pStmt.setString(1, per_id);
		ResultSet rs = pStmt.executeQuery();
		while (rs.next()) {
			String[] line = new String[3];
			line[0] = rs.getString("first_name");
			line[1] = rs.getString("last_name");
			line[2] = rs.getString("job_title");
			al.add(line);
		}
		return al;
	}

	/**
	 * Query 14 setup
	 * 
	 * @param pos_code
	 * @return
	 * @throws SQLException
	 */
	public ArrayList<String[]> queryFourteen(String pos_code) throws SQLException {
		String str = "SELECT per_id, first_name, job_title, start_date, end_date " + "FROM person natural join works "
				+ "WHERE pos_code = ? ";
		ArrayList<String[]> al = new ArrayList<String[]>();
		PreparedStatement pStmt = conn.prepareStatement(str);
		pStmt.setString(1, pos_code);
		ResultSet rs = pStmt.executeQuery();
		while (rs.next()) {
			String[] line = new String[5];
			line[0] = rs.getString("per_id");
			line[1] = rs.getString("first_name");
			line[2] = rs.getString("job_title");
			line[3] = rs.getString("start_date");
			line[4] = rs.getString("end_date");
			al.add(line);
		}
		return al;
	}

	/**
	 * Query 15 setup
	 * 
	 * @param pos_code
	 * @return
	 * @throws SQLException
	 */
	public ArrayList<String[]> queryFifteen(String pos_code) throws SQLException {
		String str = "SELECT\n" + "    \"A1\".\"PER_ID\" \"PER_ID\"\n" + "FROM\n" + "    (\n" + "        SELECT\n"
				+ "            \"A2\".\"PER_ID\"       \"PER_ID\",\n"
				+ "            \"A2\".\"POS_CODE\"     \"POS_CODE\",\n"
				+ "            \"A2\".\"START_DATE\"   \"START_DATE\",\n"
				+ "            \"A2\".\"END_DATE\"     \"END_DATE\"\n" + "        FROM\n" + "            (\n"
				+ "                ( SELECT\n" + "                    \"A6\".\"PER_ID\" \"PER_ID\"\n"
				+ "                FROM\n" + "                    \"TATRAN6\".\"PERSON\" \"A6\"\n"
				+ "                )\n" + "                MINUS\n" + "                ( SELECT\n"
				+ "                    \"A5\".\"PER_ID\" \"PER_ID\"\n" + "                FROM\n"
				+ "                    (\n" + "                        SELECT\n"
				+ "                            \"A7\".\"PER_ID\"       \"PER_ID\",\n"
				+ "                            \"A7\".\"START_DATE\"   \"START_DATE\",\n"
				+ "                            \"A7\".\"END_DATE\"     \"END_DATE\"\n"
				+ "                        FROM\n" + "                            \"TATRAN6\".\"WORKS\" \"A7\"\n"
				+ "                    ) \"A5\"\n" + "                WHERE\n"
				+ "                    \"A5\".\"START_DATE\" IS NOT NULL\n"
				+ "                    AND \"A5\".\"END_DATE\" IS NULL\n" + "                )\n"
				+ "            ) \"A3\",\n"
				+ "            \"TATRAN6\".\"WORKS\"                                                                                                                                                                                                                                                          \"A2\"\n"
				+ "        WHERE\n" + "            \"A3\".\"PER_ID\" = \"A2\".\"PER_ID\"\n" + "    ) \"A1\"\n"
				+ "WHERE\n" + "    \"A1\".\"POS_CODE\" = ?";
		ArrayList<String[]> al = new ArrayList<String[]>();
		PreparedStatement pStmt = conn.prepareStatement(str);
		pStmt.setString(1, pos_code);
		ResultSet rs = pStmt.executeQuery();
		while (rs.next()) {
			String[] line = new String[1];
			line[0] = rs.getString("per_id");
			al.add(line);
		}
		return al;
	}

	/**
	 * Query 16 setup
	 * 
	 * @return
	 * @throws SQLException
	 */
	ArrayList<String[]> querySixteen() throws SQLException {
		String str = "SELECT\n" + "    \"A3\".\"AVG_PAY\"   \"AVG_PAY\",\n" + "    \"A2\".\"MAX_PAY\"   \"MAX_PAY\",\n"
				+ "    \"A1\".\"MIN_PAY\"   \"MIN_PAY\"\n" + "FROM\n" + "    (\n" + "        SELECT\n"
				+ "            AVG(\"A4\".\"PAY_RATE\") \"AVG_PAY\"\n" + "        FROM\n" + "            (\n"
				+ "                SELECT\n" + "                    \"A15\".\"IND_ID\"      \"IND_ID\",\n"
				+ "                    \"A16\".\"PAY_RATE\"    \"PAY_RATE\",\n"
				+ "                    \"A15\".\"PARENT_ID\"   \"PARENT_ID\"\n" + "                FROM\n"
				+ "                    (\n" + "                        SELECT\n"
				+ "                            \"A17\".\"COMP_ID\"    \"COMP_ID\",\n"
				+ "                            \"A17\".\"IND_ID\"     \"IND_ID\",\n"
				+ "                            \"A18\".\"PAY_RATE\"   \"PAY_RATE\"\n" + "                        FROM\n"
				+ "                            (\n" + "                                SELECT\n"
				+ "                                    \"A19\".\"COMP_ID\"    \"COMP_ID\",\n"
				+ "                                    \"A20\".\"PAY_RATE\"   \"PAY_RATE\",\n"
				+ "                                    \"A19\".\"IND_ID\"     \"IND_ID\"\n"
				+ "                                FROM\n"
				+ "                                    \"TATRAN6\".\"JOB\"       \"A20\",\n"
				+ "                                    \"TATRAN6\".\"COMPANY\"   \"A19\"\n"
				+ "                                WHERE\n"
				+ "                                    \"A20\".\"COMP_ID\" = \"A19\".\"COMP_ID\"\n"
				+ "                            ) \"A18\",\n"
				+ "                            \"TATRAN6\".\"SUB_INDUSTRY\"                                                                                                                                                  \"A17\"\n"
				+ "                        WHERE\n"
				+ "                            \"A18\".\"IND_ID\" = \"A17\".\"IND_ID\"\n"
				+ "                            AND \"A18\".\"COMP_ID\" = \"A17\".\"COMP_ID\"\n"
				+ "                    ) \"A16\",\n"
				+ "                    \"TATRAN6\".\"GICS\"                                                                                                                                                                                                                                                                                                                                                       \"A15\"\n"
				+ "                WHERE\n" + "                    \"A16\".\"IND_ID\" = \"A15\".\"IND_ID\"\n"
				+ "            ) \"A4\"\n" + "        GROUP BY\n" + "            \"A4\".\"PARENT_ID\"\n"
				+ "    ) \"A3\",\n" + "    (\n" + "        SELECT\n"
				+ "            MAX(\"A5\".\"PAY_RATE\") \"MAX_PAY\"\n" + "        FROM\n" + "            (\n"
				+ "                SELECT\n" + "                    \"A11\".\"IND_ID\"      \"IND_ID\",\n"
				+ "                    \"A12\".\"COMP_ID\"     \"COMP_ID\",\n"
				+ "                    \"A12\".\"PAY_RATE\"    \"PAY_RATE\",\n"
				+ "                    \"A11\".\"PARENT_ID\"   \"PARENT_ID\"\n" + "                FROM\n"
				+ "                    (\n" + "                        SELECT\n"
				+ "                            \"A13\".\"COMP_ID\"    \"COMP_ID\",\n"
				+ "                            \"A14\".\"PAY_RATE\"   \"PAY_RATE\",\n"
				+ "                            \"A13\".\"IND_ID\"     \"IND_ID\"\n" + "                        FROM\n"
				+ "                            \"TATRAN6\".\"JOB\"       \"A14\",\n"
				+ "                            \"TATRAN6\".\"COMPANY\"   \"A13\"\n" + "                        WHERE\n"
				+ "                            \"A14\".\"COMP_ID\" = \"A13\".\"COMP_ID\"\n"
				+ "                    ) \"A12\",\n"
				+ "                    \"TATRAN6\".\"GICS\"                                                                                                                                                          \"A11\"\n"
				+ "                WHERE\n" + "                    \"A12\".\"IND_ID\" = \"A11\".\"IND_ID\"\n"
				+ "            ) \"A5\"\n" + "        GROUP BY\n" + "            \"A5\".\"IND_ID\"\n"
				+ "    )                                                                                                                                           \"A2\",\n"
				+ "    (\n" + "        SELECT\n" + "            MIN(\"A6\".\"PAY_RATE\") \"MIN_PAY\"\n"
				+ "        FROM\n" + "            (\n" + "                SELECT\n"
				+ "                    \"A7\".\"IND_ID\"      \"IND_ID\",\n"
				+ "                    \"A8\".\"COMP_ID\"     \"COMP_ID\",\n"
				+ "                    \"A8\".\"PAY_RATE\"    \"PAY_RATE\",\n"
				+ "                    \"A7\".\"PARENT_ID\"   \"PARENT_ID\"\n" + "                FROM\n"
				+ "                    (\n" + "                        SELECT\n"
				+ "                            \"A9\".\"COMP_ID\"     \"COMP_ID\",\n"
				+ "                            \"A10\".\"PAY_RATE\"   \"PAY_RATE\",\n"
				+ "                            \"A9\".\"IND_ID\"      \"IND_ID\"\n" + "                        FROM\n"
				+ "                            \"TATRAN6\".\"JOB\"       \"A10\",\n"
				+ "                            \"TATRAN6\".\"COMPANY\"   \"A9\"\n" + "                        WHERE\n"
				+ "                            \"A10\".\"COMP_ID\" = \"A9\".\"COMP_ID\"\n"
				+ "                    ) \"A8\",\n"
				+ "                    \"TATRAN6\".\"GICS\"                                                                                                                                                      \"A7\"\n"
				+ "                WHERE\n" + "                    \"A8\".\"IND_ID\" = \"A7\".\"IND_ID\"\n"
				+ "            ) \"A6\"\n" + "        GROUP BY\n" + "            \"A6\".\"IND_ID\"\n"
				+ "    )                                                                                                                                                       \"A1\"\n";
		ArrayList<String[]> al = new ArrayList<String[]>();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(str);
		while (rs.next()) {
			String[] line = new String[3];
			line[0] = Float.toString(rs.getFloat("avg_pay"));
			line[1] = Float.toString(rs.getFloat("max_pay"));
			line[2] = Float.toString(rs.getFloat("min_pay"));
			al.add(line);
		}
		return al;
	}
	
	/**
	 * Query 17 A
	 * @return
	 * @throws SQLException
	 */
	public ArrayList<String[]> querySeventeenA() throws SQLException {
		String str = "SELECT\n" + 
				"    SUM(\"A1\".\"NUM_EMPLOYED\") \"SUM(NUM_EMPLOYED)\",\n" + 
				"    \"A1\".\"EMPLOYER\" \"EMPLOYER\"\n" + 
				"FROM\n" + 
				"    (\n" + 
				"        SELECT\n" + 
				"            COUNT(\"A2\".\"PER_ID\") \"NUM_EMPLOYED\",\n" + 
				"            \"A2\".\"COMP_ID\"          \"EMPLOYER\",\n" + 
				"            \"A2\".\"INDUSTRY_GROUP\"   \"IND_GROUP\",\n" + 
				"            \"A2\".\"IND_ID\"           \"SUB_IND\",\n" + 
				"            \"A2\".\"PARENT_ID\"        \"PARENT_ID\"\n" + 
				"        FROM\n" + 
				"            (\n" + 
				"                SELECT\n" + 
				"                    \"A3\".\"IND_ID\"           \"IND_ID\",\n" + 
				"                    \"A4\".\"COMP_ID\"          \"COMP_ID\",\n" + 
				"                    \"A4\".\"PER_ID\"           \"PER_ID\",\n" + 
				"                    \"A4\".\"INDUSTRY_GROUP\"   \"INDUSTRY_GROUP\",\n" + 
				"                    \"A3\".\"PARENT_ID\"        \"PARENT_ID\"\n" + 
				"                FROM\n" + 
				"                    (\n" + 
				"                        SELECT\n" + 
				"                            \"A5\".\"COMP_ID\"          \"COMP_ID\",\n" + 
				"                            \"A5\".\"IND_ID\"           \"IND_ID\",\n" + 
				"                            \"A6\".\"PER_ID\"           \"PER_ID\",\n" + 
				"                            \"A6\".\"INDUSTRY_GROUP\"   \"INDUSTRY_GROUP\"\n" + 
				"                        FROM\n" + 
				"                            (\n" + 
				"                                SELECT\n" + 
				"                                    \"A7\".\"COMP_ID\"          \"COMP_ID\",\n" + 
				"                                    \"A8\".\"PER_ID\"           \"PER_ID\",\n" + 
				"                                    \"A7\".\"INDUSTRY_GROUP\"   \"INDUSTRY_GROUP\",\n" + 
				"                                    \"A7\".\"IND_ID\"           \"IND_ID\"\n" + 
				"                                FROM\n" + 
				"                                    (\n" + 
				"                                        SELECT\n" + 
				"                                            \"A9\".\"JOB_CODE\"    \"JOB_CODE\",\n" + 
				"                                            \"A9\".\"JOB_TITLE\"   \"JOB_TITLE\",\n" + 
				"                                            \"A9\".\"POS_CODE\"    \"POS_CODE\",\n" + 
				"                                            \"A10\".\"PER_ID\"     \"PER_ID\",\n" + 
				"                                            \"A9\".\"COMP_ID\"     \"COMP_ID\"\n" + 
				"                                        FROM\n" + 
				"                                            \"TATRAN6\".\"WORKS\"   \"A10\",\n" + 
				"                                            \"TATRAN6\".\"JOB\"     \"A9\"\n" + 
				"                                        WHERE\n" + 
				"                                            \"A10\".\"POS_CODE\" = \"A9\".\"POS_CODE\"\n" + 
				"                                            AND \"A10\".\"JOB_TITLE\" = \"A9\".\"JOB_TITLE\"\n" + 
				"                                            AND \"A10\".\"JOB_CODE\" = \"A9\".\"JOB_CODE\"\n" + 
				"                                    ) \"A8\",\n" + 
				"                                    \"TATRAN6\".\"COMPANY\"                                                                                                                                                                                                                                                                                 \"A7\"\n" + 
				"                                WHERE\n" + 
				"                                    \"A8\".\"COMP_ID\" = \"A7\".\"COMP_ID\"\n" + 
				"                            ) \"A6\",\n" + 
				"                            \"TATRAN6\".\"SUB_INDUSTRY\"                                                                                                                                                                                                                                                                                                                                                                                                                                                               \"A5\"\n" + 
				"                        WHERE\n" + 
				"                            \"A6\".\"IND_ID\" = \"A5\".\"IND_ID\"\n" + 
				"                            AND \"A6\".\"COMP_ID\" = \"A5\".\"COMP_ID\"\n" + 
				"                    ) \"A4\",\n" + 
				"                    \"TATRAN6\".\"GICS\"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             \"A3\"\n" + 
				"                WHERE\n" + 
				"                    \"A4\".\"IND_ID\" = \"A3\".\"IND_ID\"\n" + 
				"            ) \"A2\"\n" + 
				"        GROUP BY\n" + 
				"            \"A2\".\"COMP_ID\",\n" + 
				"            \"A2\".\"INDUSTRY_GROUP\",\n" + 
				"            \"A2\".\"IND_ID\",\n" + 
				"            \"A2\".\"PARENT_ID\"\n" + 
				"    ) \"A1\"\n" + 
				"GROUP BY\n" + 
				"    \"A1\".\"EMPLOYER\"";
		ArrayList<String[]> al = new ArrayList<String[]>();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(str);
		while (rs.next()) {
			String[] line = new String[2];
			line[0] = Integer.toString(rs.getInt("SUM(num_employed)"));
			line[1] = rs.getString("employer");
			al.add(line);
		}
		return al;
	}
	
	/**
	 * Query 17 B setup
	 * @return
	 * @throws SQLException
	 */
	public ArrayList<String[]> querySeventeenB() throws SQLException {
		String str = "SELECT\n" + 
				"    SUM(\"A1\".\"NUM_EMPLOYED\") \"SUM(NUM_EMPLOYED)\",\n" + 
				"    \"A1\".\"IND_GROUP\" \"IND_GROUP\"\n" + 
				"FROM\n" + 
				"    (\n" + 
				"        SELECT\n" + 
				"            COUNT(\"A2\".\"PER_ID\") \"NUM_EMPLOYED\",\n" + 
				"            \"A2\".\"COMP_ID\"          \"EMPLOYER\",\n" + 
				"            \"A2\".\"INDUSTRY_GROUP\"   \"IND_GROUP\",\n" + 
				"            \"A2\".\"IND_ID\"           \"SUB_IND\",\n" + 
				"            \"A2\".\"PARENT_ID\"        \"PARENT_ID\"\n" + 
				"        FROM\n" + 
				"            (\n" + 
				"                SELECT\n" + 
				"                    \"A3\".\"IND_ID\"           \"IND_ID\",\n" + 
				"                    \"A4\".\"COMP_ID\"          \"COMP_ID\",\n" + 
				"                    \"A4\".\"PER_ID\"           \"PER_ID\",\n" + 
				"                    \"A4\".\"INDUSTRY_GROUP\"   \"INDUSTRY_GROUP\",\n" + 
				"                    \"A3\".\"PARENT_ID\"        \"PARENT_ID\"\n" + 
				"                FROM\n" + 
				"                    (\n" + 
				"                        SELECT\n" + 
				"                            \"A5\".\"COMP_ID\"          \"COMP_ID\",\n" + 
				"                            \"A5\".\"IND_ID\"           \"IND_ID\",\n" + 
				"                            \"A6\".\"PER_ID\"           \"PER_ID\",\n" + 
				"                            \"A6\".\"INDUSTRY_GROUP\"   \"INDUSTRY_GROUP\"\n" + 
				"                        FROM\n" + 
				"                            (\n" + 
				"                                SELECT\n" + 
				"                                    \"A7\".\"COMP_ID\"          \"COMP_ID\",\n" + 
				"                                    \"A8\".\"PER_ID\"           \"PER_ID\",\n" + 
				"                                    \"A7\".\"INDUSTRY_GROUP\"   \"INDUSTRY_GROUP\",\n" + 
				"                                    \"A7\".\"IND_ID\"           \"IND_ID\"\n" + 
				"                                FROM\n" + 
				"                                    (\n" + 
				"                                        SELECT\n" + 
				"                                            \"A9\".\"JOB_CODE\"    \"JOB_CODE\",\n" + 
				"                                            \"A9\".\"JOB_TITLE\"   \"JOB_TITLE\",\n" + 
				"                                            \"A9\".\"POS_CODE\"    \"POS_CODE\",\n" + 
				"                                            \"A10\".\"PER_ID\"     \"PER_ID\",\n" + 
				"                                            \"A9\".\"COMP_ID\"     \"COMP_ID\"\n" + 
				"                                        FROM\n" + 
				"                                            \"TATRAN6\".\"WORKS\"   \"A10\",\n" + 
				"                                            \"TATRAN6\".\"JOB\"     \"A9\"\n" + 
				"                                        WHERE\n" + 
				"                                            \"A10\".\"POS_CODE\" = \"A9\".\"POS_CODE\"\n" + 
				"                                            AND \"A10\".\"JOB_TITLE\" = \"A9\".\"JOB_TITLE\"\n" + 
				"                                            AND \"A10\".\"JOB_CODE\" = \"A9\".\"JOB_CODE\"\n" + 
				"                                    ) \"A8\",\n" + 
				"                                    \"TATRAN6\".\"COMPANY\"                                                                                                                                                                                                                                                                                 \"A7\"\n" + 
				"                                WHERE\n" + 
				"                                    \"A8\".\"COMP_ID\" = \"A7\".\"COMP_ID\"\n" + 
				"                            ) \"A6\",\n" + 
				"                            \"TATRAN6\".\"SUB_INDUSTRY\"                                                                                                                                                                                                                                                                                                                                                                                                                                                               \"A5\"\n" + 
				"                        WHERE\n" + 
				"                            \"A6\".\"IND_ID\" = \"A5\".\"IND_ID\"\n" + 
				"                            AND \"A6\".\"COMP_ID\" = \"A5\".\"COMP_ID\"\n" + 
				"                    ) \"A4\",\n" + 
				"                    \"TATRAN6\".\"GICS\"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             \"A3\"\n" + 
				"                WHERE\n" + 
				"                    \"A4\".\"IND_ID\" = \"A3\".\"IND_ID\"\n" + 
				"            ) \"A2\"\n" + 
				"        GROUP BY\n" + 
				"            \"A2\".\"INDUSTRY_GROUP\",\n" + 
				"            \"A2\".\"COMP_ID\",\n" + 
				"            \"A2\".\"IND_ID\",\n" + 
				"            \"A2\".\"PARENT_ID\"\n" + 
				"    ) \"A1\"\n" + 
				"GROUP BY\n" + 
				"    \"A1\".\"IND_GROUP\"";
		ArrayList<String[]> al = new ArrayList<String[]>();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(str);
		while (rs.next()) {
			String[] line = new String[2];
			line[0] = Integer.toString(rs.getInt("SUM(num_employed)"));
			line[1] = rs.getString("ind_group");
			al.add(line);
		}
		return al;
	}
	
	public ArrayList<String[]> querySeventeenC() throws SQLException {
		String str = "SELECT\n" + 
				"    SUM(\"A1\".\"NUM_EMPLOYED\") \"SUM(NUM_EMPLOYED)\",\n" + 
				"    \"A1\".\"PARENT_ID\" \"PARENT_ID\"\n" + 
				"FROM\n" + 
				"    (\n" + 
				"        SELECT\n" + 
				"            \"A2\".\"PARENT_ID\"      \"PARENT_ID\",\n" + 
				"            \"A3\".\"NUM_EMPLOYED\"   \"NUM_EMPLOYED\",\n" + 
				"            \"A2\".\"IND_ID\"         \"IND_ID\"\n" + 
				"        FROM\n" + 
				"            (\n" + 
				"                SELECT\n" + 
				"                    COUNT(\"A4\".\"PER_ID\") \"NUM_EMPLOYED\",\n" + 
				"                    \"A4\".\"COMP_ID\"          \"EMPLOYER\",\n" + 
				"                    \"A4\".\"INDUSTRY_GROUP\"   \"IND_GROUP\",\n" + 
				"                    \"A4\".\"IND_ID\"           \"SUB_IND\",\n" + 
				"                    \"A4\".\"PARENT_ID\"        \"PARENT_ID\"\n" + 
				"                FROM\n" + 
				"                    (\n" + 
				"                        SELECT\n" + 
				"                            \"A5\".\"IND_ID\"           \"IND_ID\",\n" + 
				"                            \"A6\".\"COMP_ID\"          \"COMP_ID\",\n" + 
				"                            \"A6\".\"PER_ID\"           \"PER_ID\",\n" + 
				"                            \"A6\".\"INDUSTRY_GROUP\"   \"INDUSTRY_GROUP\",\n" + 
				"                            \"A5\".\"PARENT_ID\"        \"PARENT_ID\"\n" + 
				"                        FROM\n" + 
				"                            (\n" + 
				"                                SELECT\n" + 
				"                                    \"A7\".\"COMP_ID\"          \"COMP_ID\",\n" + 
				"                                    \"A7\".\"IND_ID\"           \"IND_ID\",\n" + 
				"                                    \"A8\".\"PER_ID\"           \"PER_ID\",\n" + 
				"                                    \"A8\".\"INDUSTRY_GROUP\"   \"INDUSTRY_GROUP\"\n" + 
				"                                FROM\n" + 
				"                                    (\n" + 
				"                                        SELECT\n" + 
				"                                            \"A9\".\"COMP_ID\"          \"COMP_ID\",\n" + 
				"                                            \"A10\".\"PER_ID\"          \"PER_ID\",\n" + 
				"                                            \"A9\".\"INDUSTRY_GROUP\"   \"INDUSTRY_GROUP\",\n" + 
				"                                            \"A9\".\"IND_ID\"           \"IND_ID\"\n" + 
				"                                        FROM\n" + 
				"                                            (\n" + 
				"                                                SELECT\n" + 
				"                                                    \"A11\".\"JOB_CODE\"    \"JOB_CODE\",\n" + 
				"                                                    \"A11\".\"JOB_TITLE\"   \"JOB_TITLE\",\n" + 
				"                                                    \"A11\".\"POS_CODE\"    \"POS_CODE\",\n" + 
				"                                                    \"A12\".\"PER_ID\"      \"PER_ID\",\n" + 
				"                                                    \"A11\".\"COMP_ID\"     \"COMP_ID\"\n" + 
				"                                                FROM\n" + 
				"                                                    \"TATRAN6\".\"WORKS\"   \"A12\",\n" + 
				"                                                    \"TATRAN6\".\"JOB\"     \"A11\"\n" + 
				"                                                WHERE\n" + 
				"                                                    \"A12\".\"POS_CODE\" = \"A11\".\"POS_CODE\"\n" + 
				"                                                    AND \"A12\".\"JOB_TITLE\" = \"A11\".\"JOB_TITLE\"\n" + 
				"                                                    AND \"A12\".\"JOB_CODE\" = \"A11\".\"JOB_CODE\"\n" + 
				"                                            ) \"A10\",\n" + 
				"                                            \"TATRAN6\".\"COMPANY\"                                                                                                                                                                                                                                                                                         \"A9\"\n" + 
				"                                        WHERE\n" + 
				"                                            \"A10\".\"COMP_ID\" = \"A9\".\"COMP_ID\"\n" + 
				"                                    ) \"A8\",\n" + 
				"                                    \"TATRAN6\".\"SUB_INDUSTRY\"                                                                                                                                                                                                                                                                                                                                                                                                                                                                          \"A7\"\n" + 
				"                                WHERE\n" + 
				"                                    \"A8\".\"IND_ID\" = \"A7\".\"IND_ID\"\n" + 
				"                                    AND \"A8\".\"COMP_ID\" = \"A7\".\"COMP_ID\"\n" + 
				"                            ) \"A6\",\n" + 
				"                            \"TATRAN6\".\"GICS\"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        \"A5\"\n" + 
				"                        WHERE\n" + 
				"                            \"A6\".\"IND_ID\" = \"A5\".\"IND_ID\"\n" + 
				"                    ) \"A4\"\n" + 
				"                GROUP BY\n" + 
				"                    \"A4\".\"COMP_ID\",\n" + 
				"                    \"A4\".\"INDUSTRY_GROUP\",\n" + 
				"                    \"A4\".\"IND_ID\",\n" + 
				"                    \"A4\".\"PARENT_ID\"\n" + 
				"            ) \"A3\",\n" + 
				"            \"TATRAN6\".\"GICS\"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           \"A2\"\n" + 
				"        WHERE\n" + 
				"            \"A3\".\"PARENT_ID\" = \"A2\".\"PARENT_ID\"\n" + 
				"    ) \"A1\"\n" + 
				"GROUP BY\n" + 
				"    \"A1\".\"PARENT_ID\"";
		ArrayList<String[]> al = new ArrayList<String[]>();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(str);
		while (rs.next()) {
			String[] line = new String[2];
			line[0] = Integer.toString(rs.getInt("SUM(num_employed)"));
			line[1] = rs.getString("parent_id");
			al.add(line);
		}
		return al;
	}

	/**
	 * tester Run Query here!!!
	 */
	public static void main(String[] args) throws SQLException {

		// Start of the connection to database by asking for username and password
		if (args.length == 1) {
			System.out.println("usage: java SampleQuery db-IP dp-SID");
			System.exit(1);
		}
		DatabaseConnection dbc;
		if (args.length == 0)
			dbc = new DatabaseConnection("dbsvcs.cs.uno.edu", 1521, "orcl");
		else
			dbc = new DatabaseConnection(args[0], 1521, args[1]);
		Scanner scanner = new Scanner(System.in);
		System.out.println("User Name: ");
		String username = scanner.nextLine();
		System.out.println("passcode: ");
		String dbpassword = scanner.nextLine();
		Connection conn = dbc.getDatabaseConnection(username, dbpassword);
		QueryLD sqObj = new QueryLD(conn);
		// END OF database connection process

		// Variables in MAIN
		Boolean quit = false;

		/**
		 * Loop to run the program
		 */
		while (!quit) {
			System.out.println("\n\n*****JJT LD Database Java Query Runner*****\n\n");
			System.out.println("Please enter a query number (13-28) or 0 to QUIT: ");
			// give user choice option
			try {
				int choice = scanner.nextInt();

				if (choice > 0 && choice < 13) {
					System.out.println("ERROR>>>>> You have entered the value below the given range!");
				} else if (choice == 13) {
					// Query 13 runner
					System.out.println("Running Query 13:");
					System.out.println(
							"Given a person’s identifier, find all the jobs this person is currently holding and worked\n"
									+ "in the past.");
					System.out.print("Enter a person's ID: ");
					String ans = getAnswerString();
					ArrayList<String[]> str = sqObj.queryThirteen(ans);
					for (String[] line : str) {
						System.out.printf("first_name\tlast_name\tjob_title\n%s\t\t%s\t\t%s\n\n", line[0], line[1],
								line[2]);
					}
				} else if (choice == 14) {
					// Query 14 runner
					System.out.println("Running Query 14:");
					System.out.println(
							"In a local or national crisis, we need to find all the people who once held a position of the given pos_code. List\n"
									+ "per_id, name, job title and the years the person worked in (starting year and ending year).");
					System.out.println("Enter a position code: ");
					String ans = getAnswerString();
					ArrayList<String[]> str = sqObj.queryFourteen(ans);
					for (String[] line : str) {
						System.out.printf(
								"pos_code\tfirst_name\tjob_title\t\tstart_date\t\tend_date\n%s\t\t%s\t\t%s\t%s\t%s\n\n ",
								line[0], line[1], line[2], line[3], line[4]);
					}
				} else if (choice == 15) {
					System.out.println("Running Query 15");
					System.out.println(
							"Find all the unemployed people who once held a job position of the given pos_code.");
					System.out.println("Enter position code: ");
					String ans = getAnswerString();
					System.out.println("per_id");
					ArrayList<String[]> str = sqObj.queryFifteen(ans);
					for (String[] line : str) {
						System.out.printf("%s\n\n", line[0]);
					}
				} else if (choice == 16) {
					System.out.println("Running Query 16");
					System.out.println(
							"List the average, maximum and minimum annual pay (total salaries or wage rates multiplying by 1920 hours) of each industry (listed in GICS) in the order of the industry names.");
					System.out.println("avg_pay\t\t\tmax_pay\t\t\tmin_pay");
					ArrayList<String[]> str = sqObj.querySixteen();
					for (String[] line : str) {
						System.out.printf("%s\t\t\t%s\t\t\t%s\n\n", line[0], line[1], line[2]);
					}
				} else if (choice == 17) {
					System.out.println("Which part would you like to see? A B or C");
					String part = getAnswerString();
					if (part.equalsIgnoreCase("A")) {
						System.out.println("Running Query 17 A");
						System.out.println("Find out the biggest employer in terms of number of employees.");
						System.out.println("SUM(num_employed)\t\temployer");
						ArrayList<String[]> str = sqObj.querySeventeenA();
						for (String[] line : str) {
							System.out.printf("%s\t\t\t\t%s\n\n", line[0],line[1]);
						}
					} else if (part.equalsIgnoreCase("B")) {
						System.out.println("Running Query 17 B");
						System.out.println("Find out the biggest industry in terms of number of employees.");
						System.out.println("SUM(num_employed)\t\tind_group");
						ArrayList<String[]> str = sqObj.querySeventeenB();
						for (String[] line : str) {
							System.out.printf("%s\t\t\t\t%s\n\n", line[0], line[1]);
						}
					} else if (part.equalsIgnoreCase("C")) {
						System.out.println("Running Query 17 C");
						System.out.println("Find out the biggest industry group in terms of number of employees.");
						System.out.println("SUM(num_employed)\t\tparent_id");
						ArrayList<String[]> str = sqObj.querySeventeenC();
						for (String[] line : str) {
							System.out.printf("%s\t\t\t\t%s\n\n", line[0], line[1]);
						}
					} else {
						System.out.println("ERROR >>> selction choise invaild");
					}
				}
				// This else is to check if use want to QUIT
				else if (choice == 0) {
					System.out.println("Quiting program...");
					quit = true;
				}

			} catch (InputMismatchException e) {
				System.out.println("ERROR>>>> the value must be an integer\n");
				e.printStackTrace();
				quit = true;
			}
		}
		// Closes the Scanner
		scanner.close();
	}

	/**
	 * 
	 * @return answer: which is the user input to the query as String
	 */
	public static String getAnswerString() {
		// Create new Scanner
		Scanner sc = new Scanner(System.in);
		String answer = sc.nextLine();
//		sc.close();
		return answer;
	}

	/**
	 * 
	 * @return answer: which is the user input to the query as integer
	 */
	public static int getAnswerInt() {
		// Create new Scanner
		Scanner sc = new Scanner(System.in);
		int answer = sc.nextInt();
//		sc.close();
		return answer;
	}
}
