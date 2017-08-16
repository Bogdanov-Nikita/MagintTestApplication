/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package service;

import java.math.BigInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

/**
 *
 * @author nik
 */
public class XMLHandler extends DefaultHandler{
    boolean entries =false;
    boolean entry = false;
    boolean field = false;
    //amount - хранит сумму чисел, в случае т.к. при больших числах просходят переполнения
    //используем тип BigInteger вместо int
    BigInteger amount = BigInteger.valueOf(0);
    long number = 0;
    
    
    @Override
    public void startElement(String uri, String localName,String qName,
        Attributes attributes)  {

        if (qName.equalsIgnoreCase("entries")) {
                entries = true;
        }
        if (qName.equalsIgnoreCase("entry")) {
                this.entry = true;
        }
        
        for (int index = 0; index < attributes.getLength(); index++) {
            if(attributes.getQName(index).equals("field")){
                try{
                    amount = amount.add(new BigInteger(attributes.getValue(index).trim()));
                    number++;
                }catch(NumberFormatException e){
                    Logger.getLogger(XMLHandler.class.getName()).log(Level.SEVERE, null, e);
                }
            }
        }        
    }

    @Override
    public void fatalError(SAXParseException e) throws SAXException {
        Logger.getLogger(XMLHandler.class.getName()).log(Level.SEVERE, null, e);
    }

    @Override
    public void endElement(String uri, String localName,
        String qName) throws SAXException {

        if (qName.equalsIgnoreCase("entries")) {
            entries = false;
        }
        if (qName.equalsIgnoreCase("entry")) {
            this.entry = false;
        }
        if (qName.equalsIgnoreCase("field")) {
            field = false;
        }
    }

    @Override
    public void characters(char ch[], int start, int length) throws SAXException {
    }

    public String getAmount() {
        return amount.toString();
    }

    public long getNumber() {
        return number;
    }

    public String getAverage() {
        return amount.divide(BigInteger.valueOf(number)).toString();
    }
    
}

