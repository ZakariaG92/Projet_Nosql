import com.opencsv.CSVReader;
import data.Person;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Utility {

    public static List<Person> csvToObject ()
    {


        List<Person> personList = new ArrayList<>();
        List<String[]> allRecords = new ArrayList<>();

        List<String[]> records = new ArrayList<>();
        try (CSVReader csvReader = new CSVReader(new FileReader("src/main/resources/person.csv"))) {

            for (int i=0; i<=csvReader.getLinesRead(); i++ )
            {

                records.add(csvReader.readNext());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }



        for (int j=1; j<records.size()-1; j++){

            String[] strTab = records.get(j)[0].split("\\p{Punct}");
           // allRecords.add(strTab);

            Person person = new Person();
            person.id= strTab[0];
            person.firstName=strTab[1];
            person.lastName=strTab[2];
            person.gender=strTab[3];
            person.birthday=strTab[4];
            person.creationDate=strTab[5];
            person.locationIP=strTab[6];
            person.browserUsed=strTab[7];
            person.place=Integer.parseInt(strTab[8]);

            personList.add(person);

        }

     //   String[] last=allRecords.get(0);
        String str="";

        return  personList;
    }
}
