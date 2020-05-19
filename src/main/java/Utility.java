import com.google.gson.Gson;
import com.opencsv.CSVReader;
import data.Person;
import okhttp3.OkHttpClient;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class Utility {

    public static void csvToDatabase () throws IOException {
        Gson gson = new Gson();
        final OkHttpClient httpClient = new OkHttpClient();
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



        for (int j=0; j<records.size()-2; j++){

            LocalDateTime dateTime = LocalDateTime.parse(records.get(j+1)[5].substring(0,19));
            Timestamp timestamp = Timestamp.valueOf(dateTime);
            String time= timestamp.toString();

            Person person = new Person();
            person.id= records.get(j+1)[0];
            person.firstName=records.get(j+1)[1];
            person.lastName=records.get(j+1)[2];
            person.gender=records.get(j+1)[3];
            person.birthday=records.get(j+1)[4];
            person.creationDate=time;
            person.locationIP=records.get(j+1)[6];
            person.browserUsed=records.get(j+1)[7];
            person.place=Integer.parseInt( records.get(j+1)[8]);



            URL url = new URL("http://localhost:9200/app1/customer/"+person.id);
            HttpURLConnection con = (HttpURLConnection)url.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json; utf-8");
            con.setRequestProperty("Accept", "application/json");
            con.setDoOutput(true);

            String jsonInputString = gson.toJson(person);

            try(OutputStream os = con.getOutputStream()) {
                byte[] input = jsonInputString.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            try(BufferedReader br = new BufferedReader(
                    new InputStreamReader(con.getInputStream(), "utf-8"))) {
                StringBuilder response = new StringBuilder();
                String responseLine = null;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
                System.out.println(response.toString());
            }

        }


    }
}
