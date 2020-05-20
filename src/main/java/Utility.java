import com.google.gson.Gson;
import com.opencsv.CSVReader;
import data.Person;
import data.order.Order;
import okhttp3.OkHttpClient;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class Utility {

    public static void loadCustomerCsv () throws IOException {
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

            postElasticsearch("http://localhost:9200/app2/customer/"+person.id, person);





        }


    }


    public static void postElasticsearch(String url1,Object object) throws IOException {
        Gson gson = new Gson();
        //URL url = new URL("http://localhost:9200/app1/customer/"+person.id);
        URL url = new URL(url1);
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json; utf-8");
        con.setRequestProperty("Accept", "application/json");
        con.setDoOutput(true);

        String jsonInputString = gson.toJson(object);

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


    public static void loadOrder(String path) throws IOException {


        BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
        ArrayList<Order> orders = new ArrayList<Order>();
        Gson gson = new Gson();

        StringBuilder response = new StringBuilder();
        String responseLine = null;
        while ((responseLine = bufferedReader.readLine()) != null) {
            response.append(responseLine.trim());
            String json= responseLine.trim().toString();
            Order order = gson.fromJson(json, Order.class);
            orders.add(order);
        }

        for (int i=0; i<orders.size(); i++){
            try {
                postElasticsearch("http://localhost:9200/app9/order/"+orders.get(i).orderId.toString(), orders.get(i));
                //Thread.sleep(1000);
            }
            catch (Exception e){
                System.out.println("http://localhost:9200/app9/order/"+orders.get(i).orderId.toString()+" not created");
            }
        }




        String str="";
    }
}
