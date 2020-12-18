//package com.github.aly8246.fq.format;
//
//import java.io.InputStream;
//import java.io.Reader;
//import java.math.BigDecimal;
//import java.net.URL;
//import java.sql.*;
//import java.util.ArrayList;
//import java.util.Calendar;
//
//public class SqlPreparedStatement implements PreparedStatement {
//    private final ArrayList parameterValues;
//
//    private final String sqlTemplate;
//
//
//    public SqlPreparedStatement(String sql)
//            throws SQLException {
//        // use connection to make a prepared statement
//        sqlTemplate = sql;
//        parameterValues = new ArrayList();
//    }
//
//    private void saveQueryParamValue(int position, Object obj) {
//        String strValue;
//        if (obj instanceof String || obj instanceof Date) {
//            // if we have a String, include '' in the saved value
//            strValue = "'" + obj + "'";
//        } else {
//            if (obj == null) {
//                // convert null to the string null
//                strValue = "null";
//            } else {
//                // unknown object (includes all Numbers), just call toString
//                strValue = obj.toString();
//            }
//        }
//        // if we are setting a position larger than current size of
//        // parameterValues, first make it larger
//        while (position >= parameterValues.size()) {
//            parameterValues.add(null);
//        }
//        // save the parameter
//        parameterValues.set(position, strValue);
//    }
//
//    // 这一步是对ArrayList与sql进行处理，输出完整的sql语句
//    public String getQueryString() {
//        int len = sqlTemplate.length();
//        StringBuffer t = new StringBuffer(len * 2);
//
//        if (parameterValues != null) {
//            int i = 1, limit = 0, base = 0;
//
//            while ((limit = sqlTemplate.indexOf('?', limit)) != -1) {
//                t.append(sqlTemplate, base, limit);
//                t.append(parameterValues.get(i));
//                i++;
//                limit++;
//                base = limit;
//            }
//            if (base < len) {
//                t.append(sqlTemplate.substring(base));
//            }
//        }
//        return t.toString();
//    }
//
//    public void addBatch() {
//    }
//
//    public void clearParameters() {
//    }
//
//    public boolean execute() {
//        return false;
//    }
//
//    public ResultSet executeQuery() {
//        return null;
//    }
//
//    public int executeUpdate() {
//        return 0;
//    }
//
//    public ResultSetMetaData getMetaData() {
//        return null;
//    }
//
//    public ParameterMetaData getParameterMetaData() {
//        return null;
//    }
//
//    public void setArray(int i, Array x) {
//        saveQueryParamValue(i, x);
//    }
//
//    public void setAsciiStream(int parameterIndex, InputStream x, int length) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setBigDecimal(int parameterIndex, BigDecimal x) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setBinaryStream(int parameterIndex, InputStream x, int length) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setBlob(int i, Blob x) {
//        saveQueryParamValue(i, x);
//    }
//
//    public void setBoolean(int parameterIndex, boolean x) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setByte(int parameterIndex, byte x) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setBytes(int parameterIndex, byte[] x) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setCharacterStream(int parameterIndex, Reader reader, int length) {
//        saveQueryParamValue(parameterIndex, reader);
//    }
//
//    public void setClob(int i, Clob x) {
//        saveQueryParamValue(i, x);
//    }
//
//    public void setDate(int parameterIndex, Date x) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setDate(int parameterIndex, Date x, Calendar cal) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setDouble(int parameterIndex, double x) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setFloat(int parameterIndex, float x) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setInt(int parameterIndex, int x) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setLong(int parameterIndex, long x) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setNull(int parameterIndex, int sqlType) {
//        saveQueryParamValue(parameterIndex, sqlType);
//    }
//
//    public void setNull(int paramIndex, int sqlType, String typeName) {
//        saveQueryParamValue(paramIndex, sqlType);
//    }
//
//    public void setObject(int parameterIndex, Object x) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setObject(int parameterIndex, Object x, int targetSqlType) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setRef(int i, Ref x) {
//        saveQueryParamValue(i, x);
//    }
//
//    public void setShort(int parameterIndex, short x) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setString(int parameterIndex, String x) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setTime(int parameterIndex, Time x) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setTime(int parameterIndex, Time x, Calendar cal) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setTimestamp(int parameterIndex, Timestamp x) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setURL(int parameterIndex, URL x) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void setUnicodeStream(int parameterIndex, InputStream x, int length) {
//        saveQueryParamValue(parameterIndex, x);
//    }
//
//    public void addBatch(String sql) {
//    }
//
//    public void cancel() {
//    }
//
//    public void clearBatch() {
//    }
//
//    public void clearWarnings() {
//    }
//
//    public void close() {
//    }
//
//    public boolean execute(String sql) {
//        return false;
//    }
//
//    public boolean execute(String sql, int autoGeneratedKeys) {
//        return false;
//    }
//
//    public boolean execute(String sql, int[] columnIndexes) {
//        return false;
//    }
//
//    public boolean execute(String sql, String[] columnNames) {
//        return false;
//    }
//
//    public int[] executeBatch() {
//        return null;
//    }
//
//    public ResultSet executeQuery(String sql) {
//        return null;
//    }
//
//    public int executeUpdate(String sql) {
//        return 0;
//    }
//
//    public int executeUpdate(String sql, int autoGeneratedKeys) {
//        return 0;
//    }
//
//    public int executeUpdate(String sql, int[] columnIndexes) {
//        return 0;
//    }
//
//    public int executeUpdate(String sql, String[] columnNames) {
//        return 0;
//    }
//
//    public Connection getConnection() {
//        return null;
//    }
//
//    public int getFetchDirection() {
//        return 1;
//    }
//
//    public int getFetchSize() {
//        return 0;
//    }
//
//    public ResultSet getGeneratedKeys() {
//        return null;
//    }
//
//    public int getMaxFieldSize() {
//        return 0;
//    }
//
//    public int getMaxRows() {
//        return 0;
//    }
//
//    public boolean getMoreResults() {
//        return false;
//    }
//
//    public boolean getMoreResults(int current) {
//        return false;
//    }
//
//    public int getQueryTimeout() {
//        return 0;
//    }
//
//    public ResultSet getResultSet() {
//        return null;
//    }
//
//    public int getResultSetConcurrency() {
//        return 0;
//    }
//
//    public int getResultSetHoldability() {
//        return 0;
//    }
//
//    public int getResultSetType() {
//        return 0;
//    }
//
//    public int getUpdateCount() {
//        return 0;
//    }
//
//    public SQLWarning getWarnings() {
//        return null;
//    }
//
//    public void setCursorName(String name) {
//    }
//
//    public void setEscapeProcessing(boolean enable) {
//    }
//
//    public void setFetchDirection(int direction) {
//    }
//
//    public void setFetchSize(int rows) {
//    }
//
//    public void setMaxFieldSize(int max) {
//    }
//
//    public void setMaxRows(int max) {
//    }
//
//    public void setQueryTimeout(int seconds) {
//    }
//
//    public void setAsciiStream(int parameterIndex, InputStream x) {
//    }
//
//    public void setAsciiStream(int parameterIndex, InputStream x, long length) {
//    }
//
//    public void setBinaryStream(int parameterIndex, InputStream x) {
//    }
//
//    public void setBinaryStream(int parameterIndex, InputStream x, long length) {
//    }
//
//    public void setBlob(int parameterIndex, InputStream inputStream) {
//    }
//
//    public void setBlob(int parameterIndex, InputStream inputStream, long length) {
//    }
//
//    public void setCharacterStream(int parameterIndex, Reader reader) {
//    }
//
//    public void setCharacterStream(int parameterIndex, Reader reader, long length) {
//    }
//
//    public void setClob(int parameterIndex, Reader reader) {
//    }
//
//    public void setClob(int parameterIndex, Reader reader, long length) {
//    }
//
//    public void setNCharacterStream(int parameterIndex, Reader value) {
//    }
//
//    public void setNCharacterStream(int parameterIndex, Reader value, long length) {
//    }
//
//    public void setNClob(int parameterIndex, NClob value) {
//    }
//
//    public void setNClob(int parameterIndex, Reader reader) {
//    }
//
//    public void setNClob(int parameterIndex, Reader reader, long length) {
//    }
//
//    public void setNString(int parameterIndex, String value) {
//    }
//
//    public void setRowId(int parameterIndex, RowId x) {
//    }
//
//    public void setSQLXML(int parameterIndex, SQLXML xmlObject) {
//    }
//
//    public boolean isClosed() {
//        return true;
//    }
//
//    public boolean isPoolable() {
//        return false;
//    }
//
//    @Override
//    public void closeOnCompletion() {
//    }
//
//    @Override
//    public boolean isCloseOnCompletion() {
//        return true;
//    }
//
//    public void setPoolable(boolean poolable) {
//    }
//
//    public boolean isWrapperFor(Class<?> iface) {
//        return false;
//    }
//
//    public <T> T unwrap(Class<T> iface) {
//        return null;
//    }
//
//}
