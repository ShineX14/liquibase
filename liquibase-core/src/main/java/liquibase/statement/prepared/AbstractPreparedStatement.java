package liquibase.statement.prepared;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import liquibase.change.ColumnConfig;
import liquibase.database.Database;
import liquibase.database.PreparedStatementFactory;
import liquibase.database.core.PostgresDatabase;
import liquibase.exception.DatabaseException;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.ExecutablePreparedStatement;
import liquibase.util.StreamUtil;

public abstract class AbstractPreparedStatement implements
		ExecutablePreparedStatement {

	private final Logger log = LogFactory.getInstance().getLog();
    protected Database database;

	protected List<String> getPrimaryKey(String primaryKeys) {
		if (primaryKeys == null || primaryKeys.length() == 0) {
			throw new NullPointerException();
		}

		String[] pknames = primaryKeys.split(",");
		List<String> list = new ArrayList<String>();
		for (String pk : pknames) {
          list.add(pk.trim());
        }
		return list;
	}
	
	protected String getPrimaryKeyClause(String primaryKeys, List columns,
			List<ColumnConfig> cols) {
		List<String> pknames = getPrimaryKey(primaryKeys);
		StringBuilder sql = new StringBuilder();
		for (int i = 0; i < pknames.size(); i++) {
			if (i > 0) {
				sql.append(" and ");
			}

			String pkname = pknames.get(i);
			ColumnConfig pkcolumn = getColumn(columns, pkname);
			if (pkcolumn == null) {
				throw new IllegalArgumentException(primaryKeys);
			}
			sql.append(pkname + "=?");
			cols.add(pkcolumn);
		}
		return sql.toString();
	}
	
	protected String getPrimaryKeyClause(List<String> pknames, List<String> nullpknames, String sourceTablePrefix, String targetTablePrefix) {
		StringBuilder sql = new StringBuilder();
		for (int i = 0; i < pknames.size(); i++) {
			if (i > 0) {
				sql.append(" and ");
			}

			String pkname = pknames.get(i);
			if (targetTablePrefix != null) {
				sql.append(targetTablePrefix + ".");
			}
            if (nullpknames.contains(pkname)) {
               sql.append(" is null");
            } else {
                sql.append("=");
				if (sourceTablePrefix != null) {
					sql.append(sourceTablePrefix + ".");
				}
				sql.append(pkname);
			}
		}
		return sql.toString();
	}

	protected ColumnConfig getColumn(List columns, String columnName) {
		for (Iterator i = columns.iterator(); i.hasNext();) {
			ColumnConfig column = (ColumnConfig) i.next();
			if (columnName.equals(column.getName())) {
				return column;
			}
		}
		throw new IllegalArgumentException(columnName + " not found in columns");
	}

	protected void setParameter(PreparedStatement stmt,
			List<ColumnConfig> cols, String parentFilePath,
			ResourceAccessor opener) throws SQLException, DatabaseException {
		setParameter(stmt, 1, cols, parentFilePath, opener);
	}

	protected List<String> getParameters(List<ColumnConfig> cols,
			String parentFilePath) {
		List<String> params = new ArrayList<String>();
		for (int i = 0; i < cols.size(); i++) {
			ColumnConfig col = cols.get(i);
			if (col.getValue() != null) {
				params.add(col.getValue());
			} else if (col.getValueBoolean() != null) {
				params.add(col.getValueBoolean().toString());
			} else if (col.getValueNumeric() != null) {
				params.add(col.getValueNumeric().toString());
			} else if (col.getValueDate() != null) {
				params.add(col.getValueDate().toString());
			} else if (col.getValueBlobFile() != null) {
				String value = col.getValueBlobFile();
				if (value.contains(".")) {
				    value = getFilePath(value, parentFilePath);
                }
				params.add(value);
			} else if (col.getValueClobFile() != null) {
				String value = col.getValueClobFile();
                if (value.contains(".")) {
                    value = getFilePath(value, parentFilePath);
                }
                params.add(value);
			}
		}
		return params;
	}

	protected void setParameter(PreparedStatement stmt, int paramStartAt,
			List<ColumnConfig> cols, String parentFilePath,
			ResourceAccessor opener) throws SQLException, DatabaseException {
		for (int i = 0; i < cols.size(); i++) {
			ColumnConfig col = cols.get(i);
			int paramIndex = paramStartAt + i;
			if (col.getValue() != null) {
				stmt.setString(paramIndex, col.getValue());
			} else if (col.getValueBoolean() != null) {
				stmt.setBoolean(paramIndex, col.getValueBoolean());
			} else if (col.getValueNumeric() != null) {
				Number number = col.getValueNumeric();
				if (number instanceof Long) {
					stmt.setLong(paramIndex, number.longValue());
				} else if (number instanceof Integer) {
					stmt.setInt(paramIndex, number.intValue());
				} else if (number instanceof Double) {
					stmt.setDouble(paramIndex, number.doubleValue());
				} else if (number instanceof Float) {
					stmt.setFloat(paramIndex, number.floatValue());
				} else if (number instanceof BigDecimal) {
					stmt.setBigDecimal(paramIndex, (BigDecimal) number);
				} else if (number instanceof BigInteger) {
					stmt.setInt(paramIndex, number.intValue());
				}
			} else if (col.getValueDate() != null) {
                stmt.setTimestamp(paramIndex, new java.sql.Timestamp(col.getValueDate().getTime()));
			} else if (col.getValueBlobFile() != null) {
			    try {
    			    String file = col.getValueBlobFile();
    			    if (file.contains(".")) {
    			            // File file = new File(col.getValueBlob());
    			            // stmt.setBinaryStream(i, new BufferedInputStream(new
    			            // FileInputStream(file)), (int) file.length());
    			            InputStream stream = getLobFileStream(opener, file,
    			                    parentFilePath);
    			            try {
    			                stmt.setBinaryStream(paramIndex,
    			                        new BufferedInputStream(stream));
    			            } catch (Exception ee) {
    			                stmt.setBinaryStream(paramIndex,
    			                        new BufferedInputStream(stream),
    			                        stream.available());
    			            }
                    } else {
                      if (database != null && database instanceof PostgresDatabase ) {
                        stmt.setObject(paramIndex, UUID.nameUUIDFromBytes(Hex.decodeHex(file.toCharArray())));
                      } else {
                        stmt.setBytes(paramIndex, Hex.decodeHex(file.toCharArray()));
                      }
                    }
			    } catch (IOException e) {
			        throw new DatabaseException(e.getMessage(), e); // wrap
                } catch (DecoderException e) {
                    throw new DatabaseException(e.getMessage(), e); // wrap
                }
			} else if (col.getValueClobFile() != null) {
			    String file = col.getValueClobFile();
			    if (file.contains(".")) {
			        try {
			            // File file = new File(col.getValueClob());
			            // stmt.setCharacterStream(i, new BufferedReader(new
			            // FileReader(file)), (int) file.length());
			            InputStream stream = getLobFileStream(opener, file,
			                    parentFilePath);
			            InputStreamReader streamReader = new InputStreamReader(
			                    stream, StreamUtil.getDefaultEncoding());
			            try {
			                stmt.setCharacterStream(paramIndex, new BufferedReader(
			                        streamReader));
			            } catch (Exception ee) {
			                stmt.setCharacterStream(paramIndex, new BufferedReader(
			                        streamReader), stream.available());
			            }
			        } catch (IOException e) {
			            throw new DatabaseException(e.getMessage(), e); // wrap
			        }
                } else {
                    stmt.setString(paramIndex, file);
                }
			}
		}
		log.debug("with parameters: " + getParameters(cols, parentFilePath));
	}

	private InputStream getLobFileStream(ResourceAccessor opener, String file,
			String parentFilePath) throws IOException {
		String path = getFilePath(file, parentFilePath);
		InputStream is = StreamUtil.singleInputStream(path, opener);
		if (is == null) {
			throw new IllegalArgumentException(path + " not found");
		}
		return is;
	}

	private String getFilePath(String file, String parentFilePath) {
		return StreamUtil.getFilePath(file, parentFilePath);
	}

	@Override
	public void execute(PreparedStatementFactory factory)
			throws DatabaseException {
		// create prepared statement
		PreparedStatement stmt = factory.create(getStatement().sql);
		String url = null;
		try {
			url = stmt.getConnection().getMetaData().getURL();
			setParameter(stmt);
			// trigger execution
			stmt.execute();
			stmt.close();
		} catch (SQLException e) {
			log.severe(getStatement().sql);
			log.severe("with parameters: " + getStatement().parameters);
			throw new DatabaseException("Error executing JDBC SQL on " + url + "\n" + getStatement().sql + " with parameters " + getStatement().parameters, e);
		}
	}

	@Override
	public void execute(PreparedStatement stmt) throws DatabaseException {
		try {
			setParameter(stmt);
			stmt.addBatch();
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

	public abstract void setParameter(PreparedStatement stmt)
			throws DatabaseException;

	@Override
	public boolean skipOnUnsupported() {
		return false;
	}
	
	public void deleteLastSeperator(StringBuilder s) {
		int index = s.length() - 1;
		if (index >= 0 && s.charAt(index) == ',') {
			s.deleteCharAt(index);
		}
	}
}
