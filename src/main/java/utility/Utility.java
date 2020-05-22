package utility;

import com.google.gson.Gson;
import com.opencsv.CSVReader;
import data.Person;
import data.order.Order;
import data.product.BrandByProduct;
import data.product.Product;
import data.socialNetwork.PersonKnowsPerson;
import data.vendor.Vendor;
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

            postElasticsearch("http://localhost:9200/customer/customer/"+person.id, person);





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
                postElasticsearch("http://localhost:9200/order/order/"+orders.get(i).orderId.toString(), orders.get(i));
                //Thread.sleep(1000);
            }
            catch (Exception e){
                System.out.println("http://localhost:9200/order/order/"+orders.get(i).orderId.toString()+" not created");
            }
        }




        String str="";
    }


    /*********************products***************/


    public static void loadProduct(String path) throws IOException {

        Gson gson = new Gson();

        List<String[]> products = new ArrayList<>();
        try (CSVReader csvReader = new CSVReader(new FileReader(path))) {

            for (int i=0; i<=csvReader.getLinesRead(); i++ )
            {
                products.add(csvReader.readNext());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }



        for (int j=0; j<products.size()-2; j++){

            Product product = new Product();
            product.asin=products.get(j+1)[0];
            product.title=products.get(j+1)[1];
            product.price=Float.parseFloat( products.get(j+1)[2]);
            product.imgUrl=products.get(j+1)[3];

            postElasticsearch("http://localhost:9200/product/product/"+product.asin, product);





        }
    }

    /*****************BrandByProduct************/

    public static void loadBrandByProduct(String path) throws IOException {

        Gson gson = new Gson();

        List<String[]> products = new ArrayList<>();
        try (CSVReader csvReader = new CSVReader(new FileReader(path))) {

            for (int i=0; i<=csvReader.getLinesRead(); i++ )
            {
                products.add(csvReader.readNext());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }



        for (int j=0; j<products.size()-1; j++){

            BrandByProduct brandByProduct = new BrandByProduct();
            brandByProduct.brand=products.get(j)[0];
            brandByProduct.asin=products.get(j)[1];

            postElasticsearch("http://localhost:9200/brandbyproduct/brandbyproduct/"+brandByProduct.asin, brandByProduct);





        }
    }


    /*****************Vendor************/

    public static void loadVendor(String path) throws IOException {

        Gson gson = new Gson();

        List<String[]> vendors = new ArrayList<>();
        try (CSVReader csvReader = new CSVReader(new FileReader(path))) {

            for (int i=0; i<=csvReader.getLinesRead(); i++ )
            {
                vendors.add(csvReader.readNext());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }



        for (int j=0; j<vendors.size()-2; j++){

            Vendor vendor = new Vendor();
            vendor.vendor=vendors.get(j+1)[0];
            vendor.country=vendors.get(j+1)[1];
            vendor.industry=vendors.get(j+1)[2];

            postElasticsearch("http://localhost:9200/vendor/vendor/"+vendor.vendor, vendor);





        }

    }


    /**************************Social Network***************/

/**************************PersonKnowsPerson***************************/

public static void loadPersonKnowsPerson(String path) throws IOException {

    Gson gson = new Gson();

    List<String[]> personsConnexion = new ArrayList<>();
    try (CSVReader csvReader = new CSVReader(new FileReader(path))) {

        for (int i=0; i<=csvReader.getLinesRead(); i++ )
        {
            personsConnexion.add(csvReader.readNext());
        }
    } catch (IOException e) {
        e.printStackTrace();
    }



    for (int j=0; j<personsConnexion.size()-2; j++){

        LocalDateTime dateTime = LocalDateTime.parse(personsConnexion.get(j+1)[2].substring(0,19));
        Timestamp timestamp = Timestamp.valueOf(dateTime);

        PersonKnowsPerson personKnowsPerson = new PersonKnowsPerson();
        personKnowsPerson.personId1=Long.parseLong(personsConnexion.get(j+1)[0]);
        personKnowsPerson.personId2=Long.parseLong(personsConnexion.get(j+1)[1]);
        personKnowsPerson.creationDate=timestamp.toString();


        postElasticsearch("http://localhost:9200/personknowsperson/personknowsperson", personKnowsPerson);





    }

}



}
