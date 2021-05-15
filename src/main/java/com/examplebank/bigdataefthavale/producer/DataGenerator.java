package com.examplebank.bigdataefthavale.producer;

import org.json.simple.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Timestamp;
import java.util.*;

public class DataGenerator
{
    private static int pid = 2000000;
    private static List<String> nameList = new ArrayList<>();
    private static  List<String> surNameList = new ArrayList<>();
    private static  List<String> bankList = new ArrayList<>();
    private static Random random = new Random();

    public DataGenerator() throws FileNotFoundException {
        File nameFile = new File("isimler.txt");
        File surNameFile = new File("soyisimler.txt");
        Scanner nameInput = new Scanner(nameFile);
        Scanner surNameInput = new Scanner(surNameFile);
        while (nameInput.hasNext()){
            String line = nameInput.nextLine();
            nameList.add(line);
        }
        while (surNameInput.hasNext()){
            surNameList.add(surNameInput.nextLine());
        }
    }

    public String generateData()  {

            JSONObject json = new JSONObject();
            json.put("pid",generateId());
            json.put("ptype","H");

            JSONObject account = new JSONObject();
            account.put("oid",generateId());
            account.put("fullname",generateFullName());
            account.put("iban","TR"+generateIban());

            json.put("account",account);

            JSONObject info = new JSONObject();
            info.put("fullname",generateFullName());
            info.put("iban","TR"+generateIban());
            info.put("bank",generateBank());

            json.put("info",info);

            json.put("balance",generateBalance());
            json.put("btype",generateType());
            json.put("time",generateTime());

           return json.toJSONString();

    }

    public static long generateId(){
        pid+=1;
        return pid;
    }
    public static long generateIban(){
        long num = 100000000000000000L + (long) (random.nextDouble()*999999999999999999L);
        return num;
    }
    public static String generateFullName(){
        String fullName = nameList.get(random.nextInt(nameList.size())) +" "+surNameList.get(random.nextInt(surNameList.size()));
        return fullName;
    }
    public static  String generateBank(){
        Random random = new Random();
        bankList.add("Ak Bank");
        bankList.add("Deniz Bank");
        bankList.add("Vakıf Bank");
        bankList.add("Yapı Kredi Bank");
        bankList.add("Finans Bank");
        bankList.add("QNB Bank");
        bankList.add("Garanti Bankası");
        bankList.add("Halk Bank");
        bankList.add("Ziraat Bankası");
        return bankList.get(random.nextInt(bankList.size()));
    }
    public static long generateBalance(){
        int i = random.nextInt(100000-0) + 0;
        return i;
    }
    public static String generateType(){
        List<String> types = Arrays.asList("TR", "USD", "EUR", "XAU", "JPG");
        return types.get(random.nextInt(types.size()));
    }
    public static String generateTime(){
        long offset = Timestamp.valueOf("2021-05-01 00:00:00").getTime();
        long end = Timestamp.valueOf("2021-01-01 00:00:00").getTime();
        long diff = end - offset + 1;
        Timestamp rand = new Timestamp(offset + (long)(Math.random() * diff));
        return rand.toString();
    }
}
