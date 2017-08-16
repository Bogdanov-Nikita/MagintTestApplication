/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package database;

/**
 *
 * @author Nik
 * справочные пометки
 * Dialect 3 - используются кавычки
 * Dialect 1 - не требует кавычек
 */


public class QueryBilder {
    
    public static String select(String Table,String[] columns,String whereClause, String[] selectionArgs, String groupBy,String orderBy, String limit){
                        
        String SQL = "SELECT";
        if(columns!=null){
            for (int i=0;i<columns.length;i++) {
                if(i<columns.length-1){
                    SQL = SQL+" \""+Table+"\".\""+columns[i]+"\" as \""+columns[i]+"\",";
                }else{
                    SQL = SQL+" \""+Table+"\".\""+columns[i]+"\" as \""+columns[i]+"\" ";
                }            
            }
        }else{
            SQL = SQL + " * ";
        }
        SQL = SQL + "FROM \"" +Table + "\" ";
        
        
        if(whereClause!=null){
            String where=" ";
            if(selectionArgs!=null){
                String temp[]  = whereClause.split("\\?",selectionArgs.length+1);
                for(int i=0;i<temp.length-1;i++){
                    where = where + temp[i] + " " + selectionArgs[i] + " ";
                }
                where = where + temp[temp.length-1];
            }else{
               where = " " + whereClause;  
            }
            SQL = SQL + "WHERE" + where;
        }
                
        if(groupBy!=null){
        SQL = SQL + " GROUP BY \""+groupBy+"\"";
        }
        
        if(orderBy!=null){
            SQL = SQL + " ORDER BY \""+orderBy+"\"";
        }
        if(limit!=null){
            SQL = SQL + " LIMIT " + limit;
        }
        SQL=SQL+";";   
        return SQL;
    }
    
    public static String delete(String table, ContentValues v){
        
        String SQL=" ";
        
        if(!v.isEmpty()){
            for(int i =0 ;i < v.size();i++){
                Value vv = v.get(i);
                if(i<(v.size()-1)){                   
                    SQL=SQL+vv.name+"= "+vv.value+"  AND ";
                }else{                    
                    SQL=SQL+vv.name+"= "+vv.value+" ";
                }
            }
        }
        
        return delete(table, SQL);
    }
    
    public static String delete(String table, String whereClause){
        String SQL = "DELETE FROM \""+ table + "\"";
        if(!whereClause.trim().equals("")){//if not empty string
            SQL = SQL + " WHERE "+whereClause;
        }
        SQL = SQL+";";        
        return SQL;
    }
    
    public static String insert(String table, ContentValues values) {
        String SQL = "INSERT INTO \"" + table + "\" ";
        if(!values.isEmpty()){
            String collum = "";
            String value = "";
            for(int i=0;i<values.size();i++){
                Value vv = values.get(i);
                if(i<(values.size()-1)){                   
                    collum = collum + "\"" + vv.name + "\",";
                    value = value + " '" + vv.value + "'" + ", ";
                }else{
                    collum = collum + "\"" + vv.name+"\"";
                    value = value + " '" + vv.value + "' ";
                }
            }
            SQL = SQL + "(" + collum + ") VALUES (" + value + ");";
        }
        return SQL;
    }
    
    public static String insertArray(String table, String[] tableCollums, String[][] tableArgs ){
        String SQL = "INSERT INTO \"" + table + "\" ";
        if(tableCollums != null && tableCollums.length > 0){
            String collum = "";
            for(int i=0;i<tableCollums.length;i++){
                if(i<(tableCollums.length-1)){                   
                    collum = collum + "\"" + tableCollums[i] + "\",";
                }else{
                    collum = collum + "\"" + tableCollums[i] +"\"";
                }
            }
            String value = "";
            for(int i = 0 ; i < tableArgs.length; i++ ){
                String values = "";
                for(int j = 0; j < tableArgs[i].length; j++){
                    if(j<(tableArgs[i].length-1)) {
                        values = values + " '" + tableArgs[i][j] + "'" + ", ";
                    }else{
                        values = values + " '" + tableArgs[i][j] + "' ";
                    }                    
                }
                if(i==0){
                    value = value + " VALUES ("+ values +")";
                    if(tableArgs.length > 1){value = value + ","; }
                }else{
                    if(i<(tableArgs.length-1)){                   
                        value = value + "("+ values +"), ";
                    }else{
                        value = value + "("+ values +")";
                    }
                }
            }
            SQL = SQL + "(" + collum + ")" + value + ";";
        }
        return SQL;
    }
    
    public static String update(String table, ContentValues values, String whereClause, String[] whereArgs){
        String SQL = "UPDATE \""+table+"\" SET "; 
        if(!values.isEmpty()){
            for(int i=0;i<values.size();i++){
                Value vv = values.get(i);
                if(i<(values.size()-1)){                   
                    SQL=SQL + "\"" + vv.name + "\"= '" + vv.value + "', ";
                }else{                    
                    SQL=SQL + "\"" + vv.name + "\"= '" + vv.value + "'  ";
                }
            }
                        
            if(whereClause!=null){
                String Temp = " ";    
                if(whereArgs!=null){
                    String Tempstr[] = whereClause.split("\\?",whereArgs.length+1);    
                    for(int i=0;i<Tempstr.length-1;i++){
                        Temp = Temp + Tempstr[i] + " " + whereArgs[i] + " ";
                    }
                    Temp = Temp + Tempstr[Tempstr.length-1];
                }else{
                    Temp = " " + whereClause; 
                }
                SQL = SQL + "WHERE" + Temp;    
            }
            SQL = SQL + ";";
        }
        return SQL;
    } 
}