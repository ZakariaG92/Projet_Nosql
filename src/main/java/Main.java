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
        Utility.loadProduct("C:\\Users\\Zakaria\\Documents\\MIAGE\\PROJET_NOSQL_2019_2020\\DATA\\Product\\Product.csv");

    }



}
