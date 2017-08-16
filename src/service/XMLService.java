/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;




/**
 *
 * @author nik
 */
public class XMLService {

    /**
     * @param N количество элементов N из техничекого задания
     * @param fileName имя файл для записи
     * @param doubleBuffer Буфер для хранения чисел Запихиваем данные сверху читаем снизу
     * @param flag флаг для укачания окончания вычитывания записей из базы данных
     * @param readStrings количество считываемых елементов перед записью в файл.
     * @return true если запись в файл выполненна успешно иначе false.
    */

    public static boolean writeFile(int N, String fileName,ConcurrentLinkedDeque<String> doubleBuffer, AtomicInteger flag, int readStrings){
        boolean success;
        
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(Paths.get(fileName))) {
            String buffer = "<entries>\n";

            int countReadStrings = 0;
            int sum = 0;
            do{
                String s = doubleBuffer.pollLast();
                if(s != null && !s.trim().equals("")){
                    buffer = buffer +
                            "\t<entry>\n" +
                            "\t\t<field>" + s + "</field>\n" +
                            "\t</entry>\n";
                    
                    countReadStrings++;
                    sum++;
                    //if(s.equalsIgnoreCase("1000000")){System.out.println("find element");} //debug
                    if((countReadStrings > readStrings) | (sum == N)){
                        countReadStrings = 0;
                        bufferedWriter.write(buffer);
                        buffer = "";
                    }
                }
            }while((flag.get() != 0) | !doubleBuffer.isEmpty());//условие завершения
            //System.out.println("res:"+sum); //debug  
            bufferedWriter.write("</entries>\n");
            bufferedWriter.flush();
            success = true;
        } catch (IOException ex) {
            Logger.getLogger(XMLService.class.getName()).log(Level.SEVERE, null, ex);
            success = false;
        }
        return success;
    }
    
    public static boolean transformerXSLT(String inputXSLT, String inputXML,String outputXML) {
        boolean success;//успешность завершения преобразования
        try {
            TransformerFactory factory = TransformerFactory.newInstance();
            StreamSource xslStream = new StreamSource(inputXSLT);
            Transformer transformer = factory.newTransformer(xslStream);
            StreamSource in = new StreamSource(inputXML);
            StreamResult out = new StreamResult(outputXML);
            transformer.transform(in, out);
            success = true;
        } catch (TransformerConfigurationException ex) {
            Logger.getLogger(XMLService.class.getName()).log(Level.SEVERE, null, ex);
            success = false;
        } catch (TransformerException ex) {
            Logger.getLogger(XMLService.class.getName()).log(Level.SEVERE, null, ex);
            success = false;
        }
        return success;
    }
    
}
