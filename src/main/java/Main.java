import com.google.gson.Gson;
import com.opencsv.CSVReader;
import data.Person;
import okhttp3.*;
import utility.Utility;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpRequest;
import java.text.ParseException;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException, ParseException {

      //***//Utility.loadProduct("C:\\Users\\Zakaria\\Documents\\MIAGE\\PROJET_NOSQL_2019_2020\\DATA\\Product\\Product.csv");
       //**// Utility.loadBrandByProduct("C:\\Users\\Zakaria\\Documents\\MIAGE\\PROJET_NOSQL_2019_2020\\DATA\\Product\\BrandByProduct.csv");
       //**// Utility.loadVendor("C:\\Users\\Zakaria\\Documents\\MIAGE\\PROJET_NOSQL_2019_2020\\DATA\\Vendor\\Vendor.csv");
        //**// Utility.loadPersonKnowsPerson("C:\\Users\\Zakaria\\Documents\\MIAGE\\PROJET_NOSQL_2019_2020\\DATA\\SocialNetwork\\person_knows_person_0_0.csv");
       //**// Utility.loadCustomerCsv();
        //Utility.loadOrder("C:\\Users\\Zakaria\\Documents\\MIAGE\\PROJET_NOSQL_2019_2020\\DATA\\Order\\Order.json");

       // Utility.query4();
      // Utility.query7("Olympikus");
       //Utility.query5("4149","Sports");
       
    	Utility.query8("Sports");

    }



}
