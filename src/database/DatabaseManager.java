/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package database;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Nik
 */
public class DatabaseManager {
    Driver driver;
    Connection connection;
    Statement statement;
    
    public DatabaseManager(String DriverClassName, int TimeoutSeconds){
        try {
            DriverManager.registerDriver(driver = (Driver) Class.forName(DriverClassName).newInstance());
            DriverManager.setLoginTimeout(TimeoutSeconds);
        } catch (SQLException | InstantiationException | IllegalAccessException | ClassNotFoundException ex) {
            Logger.getLogger(DatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException("Can't register driver!");
        }
    }
    
    public void connect(String Url) throws SQLException{
        try{
            connection = DriverManager.getConnection(Url);
        }
        catch(SQLException e){
            connection.close();
            throw new SQLException(e.getMessage());
        }
    }
    
    public String[] getTableNames() throws SQLException{
        
        String TableNames[];
        ResultSet rs = connection.getMetaData().getTables(null, null, "%",new String[]{"TABLE"});
        if(rs.last()){
            TableNames = new String[rs.getRow()];
            rs.beforeFirst();
            for(int i=0;rs.next();i++) {
                TableNames[i] = rs.getString("TABLE_NAME");
            }
            return TableNames;
        }else{
            return null;
        }                
    }
    
    /**
     * and you get valid rowcoun
     * @param TableName - name of table
     * @return - Colum's Name in table
     * @throws java.sql.SQLException - if Transaction fail*/
    
    public String[] getColumName (String TableName)throws SQLException{
        ResultSet rs = executeQuery(QueryBilder.select(TableName, null, null, null, null, null, null));
        int size = rs.getMetaData().getColumnCount();
        String ColumName[] = new String[size];
        for(int i=1;i<=size;i++){
            ColumName[i-1]=rs.getMetaData().getColumnName(i);
        }
        return ColumName;
    }
//    
//    public int getCurrentTableLength(){
//        return length;
//    }
//    
//    public String[][] getTableData(String TableName,String ColumName[])throws SQLException{
//        ResultSet rs = executeQuery(QueryBilder.select(TableName, ColumName, null, null, null, null));
//        String TableData[][] = new String[length][ColumName.length];
//        int k = 0;
//        while(rs.next()){                    
//            for(int i=1;i<=ColumName.length;i++){
//                TableData[k][i-1] = rs.getString(i);
//            }
//            k++;                    
//        }
//        return TableData;
//    }
    
    public DatabaseMetaData getMetaData() throws SQLException{
        return connection.getMetaData();
    }
    
    public void startTransaction() throws SQLException{        
        try{                    
            connection.setAutoCommit(false);
            statement = connection.createStatement();            
        }
        catch(SQLException e){
            statement.close();
            throw new SQLException(e.getMessage());
        }    
    }
  
    /**
     *
     * @param Isolation one of the following <code>connection</code> constants:
     *        <code>connection.TRANSACTION_READ_UNCOMMITTED</code>,
     *        <code>connection.TRANSACTION_READ_COMMITTED</code>,
     *        <code>connection.TRANSACTION_REPEATABLE_READ</code>, or
     *        <code>connection.TRANSACTION_SERIALIZABLE</code>.
     *        (Note that <code>connection.TRANSACTION_NONE</code> cannot be used
     *        because it specifies that transactions are not supported.)
     * @throws SQLException if a database access error occurs, this
 method is called on a closed connection
            or the given parameter is not one of the <code>connection</code>
            constants
     */
    
    public void startTransaction(int Isolation) throws SQLException{        
        try{
            connection.setTransactionIsolation(Isolation);
            connection.setAutoCommit(false);
            statement = connection.createStatement();            
        }
        catch(SQLException e){
            statement.close();
            throw new SQLException(e.getMessage());
        }    
    }
    
    public void rollbackTransaction() throws SQLException{
        connection.rollback();        
    }
    
    public void commitTransaction() throws SQLException{
        connection.commit();
    }    
    
    public ResultSet executeQuery(String executeQuery) throws SQLException{
        ResultSet resultset = null;
        try{
            resultset = statement.executeQuery(executeQuery);                        
        }
        catch(SQLException e){
            connection.rollback();
            throw new SQLException(e.getMessage());
        } 
        return resultset;
    }
    
    public boolean execute(String executeQuery) throws SQLException{
        boolean resultset = false;
        try{
            resultset = statement.execute(executeQuery);                        
        }
        catch(SQLException e){
            connection.rollback();
            throw new SQLException(e.getMessage());
        } 
        return resultset;
    }
    
    public int update(String executeQuery) throws SQLException{
        int res = -1;
        try{
            res = statement.executeUpdate(executeQuery);                        
        }
        catch(SQLException e){
            connection.rollback();
            throw new SQLException(e.getMessage());
        } 
        return res;
    }
    
    public void closeConnection() throws SQLException{
        connection.close();
    }
    
    //Only one before next use, you mast create new Database Maneger .
    public void close(){
        try {
            DriverManager.deregisterDriver(driver);
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
