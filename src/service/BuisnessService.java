/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package service;

import bean.MainBean;
import database.DatabaseManager;
import database.QueryBilder;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.SAXException;

/**
 *
 * @author nik
 */
public class BuisnessService {
    
    static String XLSTFile = "stylesheet.xslt";
    static String inputFile = "1.xml";
    static String outputFile = "2.xml";
    static String driverClassName = "org.postgresql.Driver";
    static int timeoutSeconds = 120;
    static String tableName = "public\".\"TEST";
    static String tableFiledsName[] = {"FIELD"};
    //numberBufferLines параметр после скольки элеменов идёт запись из буфера в метод write
    //подобие размера вторичного буфера подробности смотри в методе XMLService.writeFile.
    //является настраеваемым параметром для тюнинга скорости записи в файл
    static int numberBufferLines = 100;
        
    /**
     * Очистка таблицы от прошлых записей 
     * 
     * @param Url базы данных и параметры подключения,
     * пример записи: jdbc:postgresql://localhost/testdb?user=postgres&password=postgres
     * @return true в случае успешного выполнения иначе false
     */
    public static boolean clearOldRecord(String Url){
        boolean success;
        DatabaseManager manager = new DatabaseManager(driverClassName, timeoutSeconds);
        try {
            manager.connect(Url/*getConnectionDatabaseArgumet()*/);            
            manager.startTransaction(Connection.TRANSACTION_SERIALIZABLE);
            manager.execute(QueryBilder.delete(tableName,""));
            manager.commitTransaction();
            success = true;
        }catch (SQLException ex) {
            try {
                manager.rollbackTransaction();
            } catch (SQLException ex1) {
                /* данная картина с вложенными try-catch не есть хорошо,
                   но временное решение т.к. поведение в случе ошибок 
                   небыло обозначенно в техническом задании.
                */
                Logger.getLogger(MainBean.class.getName()).log(Level.SEVERE, null, ex1);
            }
            Logger.getLogger(MainBean.class.getName()).log(Level.SEVERE, null, ex);
            success = false;
        }
        return success;
    }
    
    /** Запись данных в таблицу TEST
     * 
     * @param Url базы данных и параметры подключения,
     * пример записи: jdbc:postgresql://localhost/testdb?user=postgres&password=postgres
     * @param values массив аргументов где первый индекс номер строки, а второй индекс номер аргумента
     * пример: String[номер строки][номер аргумента] = значение аргумента.
     * @return 
     */
    public static boolean writeRecordsIntoDatabase(String Url, String[][] values){
        boolean success;
        DatabaseManager manager = new DatabaseManager(driverClassName, timeoutSeconds);
        try {
            manager.connect(Url);
            manager.startTransaction(Connection.TRANSACTION_SERIALIZABLE);
            manager.execute(QueryBilder.insertArray(tableName , tableFiledsName, values));
            manager.commitTransaction();
            success = true;
        } catch (SQLException ex) {
            try {
                manager.rollbackTransaction();
            } catch (SQLException ex1) {
                Logger.getLogger(MainBean.class.getName()).log(Level.SEVERE, null, ex1);
            }
            Logger.getLogger(MainBean.class.getName()).log(Level.SEVERE, null, ex);
            success = false;
        }
        return success;
    }
    
    //получение даных из TEST.FIELD
    public static boolean readRecords(String Url, ConcurrentLinkedDeque<String> FiledsArray, int firstRow, int pageSize){
        boolean success;
        DatabaseManager manager = new DatabaseManager(driverClassName, timeoutSeconds);

        try {
            manager.connect(Url);
            //получение даных из TEST.FIELD переписать при многопоточности 
            //на единакратный запрос и удаление 
            //попсмотреть возиожность взятия SELECT TOP для пакетной работы
            manager.startTransaction(Connection.TRANSACTION_SERIALIZABLE);
            String limitArg = String.valueOf(pageSize) + " OFFSET " + String.valueOf(firstRow);
            ResultSet rs = manager.executeQuery(QueryBilder.select(tableName,tableFiledsName,null,null,null,tableFiledsName[0],limitArg));
            while(rs.next()){
                FiledsArray.offerFirst(String.valueOf(rs.getInt(1)));
            }
            //очистка происходит при создании записей тут не нужно,
            //если нужно можно удалять то в цикле каждую запись но это затратно
            //удалять целиком нелья потеряем не обработанные данные
            manager.commitTransaction();
            success = true;
        } catch (SQLException ex) {
            try {
                manager.rollbackTransaction();
            } catch (SQLException ex1) {
                Logger.getLogger(MainBean.class.getName()).log(Level.SEVERE, null, ex1);
            }
            Logger.getLogger(MainBean.class.getName()).log(Level.SEVERE, null, ex);
            success = false;
        }
        return success;
    } 
    
    //формируем XML-документ 1.xml
    public static boolean writeXML(int N, ConcurrentLinkedDeque<String> FiledsArray, AtomicInteger flag){
        return XMLService.writeFile(N, inputFile, FiledsArray,flag, numberBufferLines);
    }
    
    //формируем XML-документ 2.xml
    public static boolean transformXML(){        
        return XMLService.transformerXSLT(XLSTFile,inputFile, outputFile);
    }
    
    //парсим XML-документ 2.xml
    public static boolean parseXML(){        
        boolean success;
        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser saxParser;
        try {
            saxParser = factory.newSAXParser();
            XMLHandler handler = new XMLHandler();
            saxParser.parse(outputFile, handler);            
            System.out.println("Average " + handler.getAverage());
            //System.out.println("Amount " + handler.getAmount());
            success = true;
        } catch(SAXException |ParserConfigurationException |IOException e){
            Logger.getLogger(MainBean.class.getName()).log(Level.WARNING, "Can't find or open " + outputFile + " file or file is illegal",e);
            success = false;
        }
        return success;
    }
    
}
