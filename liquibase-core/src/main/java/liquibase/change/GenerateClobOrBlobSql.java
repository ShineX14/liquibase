package liquibase.change;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import liquibase.statement.core.RawSqlStatement;

import org.apache.commons.codec.binary.Base64;

public class GenerateClobOrBlobSql {
	public static List<String> generateBlobSql(InputStream is, String tableName,
			String columnName, String conditionSql) {
		final int BLOB_PLSQL_APPEND_BUFFER_SIZE = 500;
		List<String> blobSqls = new ArrayList<String>();
		try {
			String sqlComment = "-- insert blob data";
			blobSqls.add(sqlComment);
			String sqlClear = "update " + tableName + " set " + columnName
					+ "=empty_blob() " + conditionSql;
			blobSqls.add(sqlClear);
			String selectSQL = "select " + columnName + " from " + tableName
					+ " " + conditionSql;
			selectSQL = "'" + escapeOracleStringValue(selectSQL) + "'";

			byte[] buff = new byte[BLOB_PLSQL_APPEND_BUFFER_SIZE];
			while (true) {
				int len = is.read(buff, 0, BLOB_PLSQL_APPEND_BUFFER_SIZE);
				if (len <= 0) {
					break;
				}
				// new buff,whose size equals the real data length
				byte[] newBuff = new byte[len];
				System.arraycopy(buff, 0, newBuff, 0, len);
				byte[] base64Data = Base64.encodeBase64(newBuff);
				String s = new String(base64Data);

				String appendSQL = "exec PKG_TOOL_DATA_IMP_EXP.P_WRITE_BLOB("
						+ selectSQL + "," + len + ",'" + s + "')";
				blobSqls.add(appendSQL);
			}
			is.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return blobSqls;
	}

	public static List<String> generateClobSql(InputStream is, String tableName,
			String columnName, String conditionSql) {
		List<String> clobSqls = new ArrayList<String>();
		try {
			StringBuffer bufContext = new StringBuffer();
			InputStreamReader streamReader = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(streamReader);
			String context = br.readLine();
			while (context != null) {
				bufContext.append(context + "\n");
				context = br.readLine();
			}
			br.close();
			streamReader.close();
			String sqlComment = "-- insert clob data";
			clobSqls.add(sqlComment);
			String sqlClear = "update " + tableName + " set " + columnName
					+ "=empty_clob() " + conditionSql;
			clobSqls.add(sqlClear);
			int maxStringSize = 200;
			int pos = 0;
			int dataLength = bufContext.length();
			while (pos < dataLength) {
				int curSize;
				if (dataLength - pos < maxStringSize) {
					curSize = dataLength - pos;
				} else {
					curSize = maxStringSize;
				}
				String appData = bufContext.substring(pos, pos + curSize);
				String sql = "update " + tableName + " set " + columnName
						+ "=concat(" + columnName + ",'" + escapeSql(appData)
						+ "') " + conditionSql;
				clobSqls.add(sql);
				pos = pos + curSize;
			}
			is.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return clobSqls;
	}

	private static String escapeSql(String str) {
		if (str == null) {
			return null;
		}
		return str.replace("'", "''");
	}

	private static String escapeOracleStringValue(String s) {
		String result = escapeSql(s);
		// replace ';' , because we treat a sql statement finished when a line
		// end with ';'
		result = result.replace(";", ";'||'");
		return result;
	}

	public static List<RawSqlStatement> sqlToRawSqlStatements(List<String> sqls) {
		List<RawSqlStatement> rawSqlStatements = new ArrayList<RawSqlStatement>();
		for (String sql : sqls) {
			RawSqlStatement rawSqlStatement = new RawSqlStatement(sql);
			rawSqlStatements.add(rawSqlStatement);
		}

		return rawSqlStatements;
	}

}
