/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bean;

import java.io.Serializable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import service.BuisnessService;
import service.InputData;

/**
 *
 * @author nik
 */
public class MainBean implements Serializable{
    private int N;
    private String connectionDatabaseArgumet;

    public void setConnectionDatabaseArgumet(String ConnectionDatabaseArgumet) {
        this.connectionDatabaseArgumet = ConnectionDatabaseArgumet;
    }

    public String getConnectionDatabaseArgumet() {
        return connectionDatabaseArgumet;
    }
    
    public void setN(int N) {
        this.N = N;
    }

    public int getN() {
        return N;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        MainBean that = (MainBean) obj;

        if (N != that.N) {
            return false;
        }
        return !(connectionDatabaseArgumet != null ? 
                !connectionDatabaseArgumet.equals(that.connectionDatabaseArgumet) : 
                that.connectionDatabaseArgumet != null);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
	result = prime * result + connectionDatabaseArgumet != null ? 
                connectionDatabaseArgumet.hashCode() : 0;
	result = prime * result + N;
        return result;
    }

    @Override
    public String toString() {
        return "MainBean{" +
                "N='" + N + '\'' +
                ", ConnectionDatabaseArgumet=" + connectionDatabaseArgumet +
                '}';
    }
    //finish code
    static int SUCSESS = 0;
    static int CLEAR_OLD_RECORD_ERROR = -1; 
    static int WRITE_RECORDS_INTO_DATABASE_ERROR = -2;
    static int READ_RECORDS_ERROR = -3;
    static int WRITE_XML_ERROR = -4;
    static int TRANSFORM_XML_ERROR = -5;
    static int PARSE_XML_ERROR = -6;
    
    public void main(){
        //SQLPackage - значение не окончательное и зависит от размера максимального 
        //количества символов в SQL транзакции к базе данных.
        //является настраеваемым параметром для тюнинга
        //скорости записи в базу данных и считывания из неё
        int SQLPackage = 10000;
        AtomicInteger finishCode = new AtomicInteger(SUCSESS);
        AtomicInteger flag = new AtomicInteger(0);
        ExecutorService executor = Executors.newWorkStealingPool();
        ConcurrentLinkedDeque<String> FiledsArray = new ConcurrentLinkedDeque<>();
        
        try {
            
            //step 1
            Future<Integer> executeCode =  executor.submit(() -> 
                    BuisnessService.clearOldRecord(connectionDatabaseArgumet) ? 
                            SUCSESS : CLEAR_OLD_RECORD_ERROR);
            if(executeCode.get()!= SUCSESS){
                throw new ExecutionException(new Throwable("CLEAR_OLD_RECORD fail"));
            }
            //System.out.println("CLEAR_OLD_RECORD pass");//debug
            
            //step 2 разбивка по SQLPackage записей на задачу ждёт завершения задачи step 1
            //генерация входных данных для sql запроса
            String array[][][] = InputData.batchingInputData(N, SQLPackage);            
            Future<Integer> executeCode2[] = new Future[array.length];
            for(int i = 0; i < executeCode2.length; i++){
                final int ix = i;
                executeCode2[i] = executor.submit(() -> {
                    final int index;index = ix;
                    return BuisnessService.writeRecordsIntoDatabase(connectionDatabaseArgumet, array[index]) ?
                            SUCSESS : WRITE_RECORDS_INTO_DATABASE_ERROR;
                });
            }
            for(Future<Integer> f : executeCode2) {
                if(f.get()!= SUCSESS){
                    throw new ExecutionException(new Throwable("WRITE_RECORDS_INTO_DATABASE fail"));
                }
            }
            //System.out.println("WRITE_RECORDS_INTO_DATABASE pass");//debug

            //step 3
            Future executeCode3[] = new Future[array.length];
            for(int i = 0; i < executeCode3.length; i++){
                final int ix = i;
                executeCode3[i] = executor.submit(() -> {
                    final int index = ix;
                    final int startIndex = array[index].length * index;
                    final int pageSize = array[index].length;
                    flag.incrementAndGet();
                    Integer exitCode = BuisnessService.readRecords(connectionDatabaseArgumet, FiledsArray, startIndex, pageSize) ? 
                        SUCSESS : READ_RECORDS_ERROR;
                    flag.decrementAndGet();
                    return exitCode;
                });
            }
            
            //step 4 не ждёт окончания step 3
            Future<Integer> executeCode4 =  executor.submit(() -> 
                    BuisnessService.writeXML(N,FiledsArray, flag) ? 
                            SUCSESS : WRITE_XML_ERROR);
            
            for(Future<Integer> f : executeCode3) {
                if(f.get()!= SUCSESS){
                    throw new ExecutionException(new Throwable("READ_RECORDS fail"));
                }
            }
            //System.out.println("READ_RECORDS pass");//debug
            
            if(executeCode4.get()!= SUCSESS){
                throw new ExecutionException(new Throwable("WRITE_XML fail"));
            }
            //System.out.println("WRITE_XML pass");//debug

            //step 5 ждёт до выполнения конца step 4
            Future<Integer> executeCode5 =  executor.submit(() -> 
                    BuisnessService.transformXML() ? 
                            SUCSESS : TRANSFORM_XML_ERROR);
            if(executeCode5.get()!= SUCSESS){
                throw new ExecutionException(new Throwable("TRANSFORM_XML fail"));
            }
            //System.out.println("TRANSFORM_XML pass");//debug

            //step 6 ждёт выполнения
            Future<Integer> executeCode6 =  executor.submit(() -> 
                    BuisnessService.parseXML() ? 
                            SUCSESS : PARSE_XML_ERROR);
            if(executeCode6.get()!= SUCSESS){
                throw new ExecutionException(new Throwable("PARSE_XML fail"));
            }
            //System.out.println("PARSE_XML pass");//debug

            executor.shutdown();
        } catch (InterruptedException | ExecutionException ex) {
            Logger.getLogger(MainBean.class.getName()).log(Level.SEVERE,"NOT EXECUTED TASK: "); 
            Logger.getLogger(MainBean.class.getName()).log(Level.SEVERE, null, ex);
            executor.shutdownNow().stream().forEach((elem) -> {
                Logger.getLogger(MainBean.class.getName()).log(Level.SEVERE,elem.toString());
            });
        }
        if(finishCode.get() != SUCSESS){
            System.err.println("Execute was interupted error code: " + finishCode.get());
        }
    }
    
}
