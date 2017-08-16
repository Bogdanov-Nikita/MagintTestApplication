/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package service;

/**
 *
 * @author nik
 */
public class InputData {
    public static String[][][] batchingInputData(int N, int packageSize){
        String tableArgs[][][];
        if(N > packageSize){
            int parts = N / packageSize;
            int modulo = N % packageSize;
            parts = (modulo > 0) ? (parts + 1) : parts;
            tableArgs = new String[parts][][];
            for(int i = 0; i < tableArgs.length; i++){
                int length = (N > packageSize) ? packageSize : N;
                N = N - packageSize;    
                tableArgs[i] = new String[length][1];
                for(int j = 0; j < tableArgs[i].length; j++){
                    tableArgs[i][j][0] = String.valueOf((i * packageSize) + (j + 1));
                }
            }
        }else{
            tableArgs = new String[1][N][1];
            for(int j = 0; j < tableArgs[0].length; j++){
                tableArgs[0][j][0] = String.valueOf(j+1);
            }
        }        
        return tableArgs;
    }
}
