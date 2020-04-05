import com.opencsv.CSVReader;
import data.Person;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Main {

    public static void main(String[] args) {
      List<Person> people=  Utility.csvToObject();
    }



}
