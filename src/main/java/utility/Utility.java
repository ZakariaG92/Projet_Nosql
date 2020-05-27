package utility;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.opencsv.CSVReader;
import data.Person;
import data.order.Order;
import data.product.BrandByProduct;
import data.product.Product;
import data.socialNetwork.PersonKnowsPerson;
import data.vendor.Vendor;
import okhttp3.OkHttpClient;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

public class Utility {

    private static String BASE_URL="https://c0c2020a12d44546a0a25129e7a11177.europe-west3.gcp.cloud.es.io:9243/";

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

            postElasticsearch(BASE_URL+"person/_doc/"+person.id, person);





        }


    }


    public static void postElasticsearch(String url1,Object object) throws IOException {
        Gson gson = new Gson();
        //URL url = new URL("http://localhost:9200/app1/customer/"+person.id);
        URL url = new URL(url1);
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Authorization","Basic ZWxhc3RpYzpuNWZ1NzN0cVlOMVlmVHBNSU16akVlMXI=");
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


    public static String postQuery(String url1,String query) throws IOException {
        String responseString;
        Gson gson = new Gson();
        //URL url = new URL("http://localhost:9200/app1/customer/"+person.id);
        URL url = new URL(url1);
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Authorization","Basic ZWxhc3RpYzpuNWZ1NzN0cVlOMVlmVHBNSU16akVlMXI=");
        con.setRequestProperty("Content-Type", "application/json; utf-8");
        con.setRequestProperty("Accept", "application/json");
        con.setDoOutput(true);

       // String jsonInputString = gson.toJson(query);

        try(OutputStream os = con.getOutputStream()) {
            byte[] input = query.getBytes("utf-8");
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
            responseString=response.toString();
        }
        return responseString;
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
                postElasticsearch(BASE_URL+"order/_doc/"+orders.get(i).orderId.toString(), orders.get(i));
                //Thread.sleep(1000);
            }
            catch (Exception e){
                System.out.println(BASE_URL+"order/_doc/"+orders.get(i).orderId.toString()+" not created");
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

            postElasticsearch(BASE_URL+"product/_doc/"+product.asin, product);





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

            postElasticsearch(BASE_URL+"brandbyproduct/_doc/"+brandByProduct.asin, brandByProduct);





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

            postElasticsearch(BASE_URL+"vendor/_doc/"+vendor.vendor, vendor);





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


        postElasticsearch(BASE_URL+"personknowsperson/_doc", personKnowsPerson);





    }

}


/******query********/

public static ArrayList<String> query4() throws IOException {

    ArrayList<String> keys= new ArrayList();
    ArrayList<Double> values= new ArrayList();
    ArrayList<String> depensesPersonnes= new ArrayList();
    ArrayList<String> commonFriends= new ArrayList();
    ArrayList<String> commonFriends2= new ArrayList();
    ArrayList<String> commonFriends3= new ArrayList();

    ArrayList<String> duplicate= new ArrayList();

    /********la requete qui permet de trouver les personne qui ont une facture*/
   String query="{\n" +
           "  \"size\" : 0,\n" +
           "    \"aggs\" : {\n" +
           "        \"genres\" : {\n" +
           "            \"terms\" : {\n" +
           "                \"field\" : \"PersonId.keyword\"\n" +
           "            }\n" +
           "        }\n" +
           "    }\n" +
           "}";
    /********on poste la requete*/
  String response= postQuery(BASE_URL+"order/_doc/_search",query);

    JSONObject jsonObject = new JSONObject(response);


Object agg= jsonObject.get("aggregations");
JSONArray aggreg= jsonObject.getJSONObject("aggregations").getJSONObject("genres").getJSONArray("buckets");


    /********on récupére toutes les clés et on les ajoute sur une liste*/
for (int i=0; i<aggreg.length();i++){
    Object id =aggreg.get(i);
    String nmb= id.toString();
   JSONObject jsonObject1 = new JSONObject(nmb);
   String key= jsonObject1.get("key").toString();
   keys.add(key);
}

    /********on fait les requetes pour chaque clés avec et nous retourne les clés avec le total depensés *****/
    for (int i=0; i<keys.size();i++){
        String query1="{\"size\":0,\"query\":{\"constant_score\":{\"filter\":{\"term\":{\"PersonId.keyword\":\""+keys.get(i)+"\"}}}},\"aggs\":{\"genres\":{\"sum\":{\"field\":\"TotalPrice\"}}}}";
        String response1=  postQuery(BASE_URL+"order/_doc/_search",query1);
        JSONObject jsonObject1 = new JSONObject(response1);
        values.add( jsonObject1.getJSONObject("aggregations").getJSONObject("genres").getDouble("value"));
    }

    /********on trouve les 2 valeurs maximum et et ajoute leurs identifiant sur une liste depensesPersonnes */
    for (int i=0; i<2;i++) {
        Double maxVal = Collections.max(values);
        int element = values.indexOf(maxVal);
        depensesPersonnes.add(keys.get(element));
        keys.remove(element);
        values.remove(element);
    }

    /********on trouve les amis de ces personnes et on les ajoute sur une liste commonFriends sans duplication*/

    for (int i=0; i<depensesPersonnes.size();i++) {
       String queryknows= "{\"size\" : 20,\"query\":{\"bool\":{\"should\":[{\"term\":{\"personId1\":\""+depensesPersonnes.get(i)+"\"}},{\"term\":{\"personId2\":\""+depensesPersonnes.get(i)+"\"}}]}}}";
     String responseKnows=  postQuery(BASE_URL+"personknowsperson/_doc/_search",queryknows);

        JSONObject jsonObjectKnows = new JSONObject(responseKnows);
        int returntab= jsonObjectKnows.getJSONObject("hits").getJSONArray("hits").length();

        for (int j=0; j<returntab;j++) {
            try {
                Long personid1 = jsonObjectKnows.getJSONObject("hits").getJSONArray("hits").getJSONObject(j).getJSONObject("_source").getLong("personId1");
                Long personid2 = jsonObjectKnows.getJSONObject("hits").getJSONArray("hits").getJSONObject(j).getJSONObject("_source").getLong("personId2");
                if (personid1.toString().equals(depensesPersonnes.get(i))) {}//commonFriends.add(personid1.toString());
                else commonFriends.add(personid1.toString());
                if (personid2.toString().equals(depensesPersonnes.get(i))) {}//commonFriends.add(personid1.toString());
                else commonFriends.add(personid2.toString());

                LinkedHashSet<String> hashSet = new LinkedHashSet<>(commonFriends);
                commonFriends = new ArrayList(hashSet);


            }catch (Exception e){}

        }



    }


    /********on trouve les amis des amis des personnes et on les ajoute sur une liste commonFriends2 sans duplication*/

    for (int i=0; i<commonFriends.size();i++) {
        String queryknows2= "{\"size\" : 20,\"query\":{\"bool\":{\"should\":[{\"term\":{\"personId1\":\""+commonFriends.get(i)+"\"}},{\"term\":{\"personId2\":\""+commonFriends.get(i)+"\"}}]}}}";
        String responseKnows2=  postQuery(BASE_URL+"personknowsperson/_doc/_search",queryknows2);

        JSONObject jsonObjectKnows2 = new JSONObject(responseKnows2);
        int returntab2= jsonObjectKnows2.getJSONObject("hits").getJSONArray("hits").length();

        for (int j=0; j<returntab2;j++) {
            try {
                Long personid1 = jsonObjectKnows2.getJSONObject("hits").getJSONArray("hits").getJSONObject(j).getJSONObject("_source").getLong("personId1");
                Long personid2 = jsonObjectKnows2.getJSONObject("hits").getJSONArray("hits").getJSONObject(j).getJSONObject("_source").getLong("personId2");
                if (personid1.toString().equals(commonFriends.get(i))) {}//commonFriends.add(personid1.toString());
                else commonFriends2.add(personid1.toString());
                if (personid2.toString().equals(commonFriends.get(i))) {}//commonFriends.add(personid1.toString());
                else commonFriends2.add(personid2.toString());

                LinkedHashSet<String> hashSet = new LinkedHashSet<>(commonFriends2);
                commonFriends2 = new ArrayList(hashSet);


            }catch (Exception e){}

        }

    }

    /********on trouve les amis des amis des amis des personnes et on les ajoute sur une liste commonFriends3 sans duplication*/

    for (int i=0; i<commonFriends2.size();i++) {
        String queryknows3= "{\"size\" : 10,\"query\":{\"bool\":{\"should\":[{\"term\":{\"personId1\":\""+commonFriends2.get(i)+"\"}},{\"term\":{\"personId2\":\""+commonFriends2.get(i)+"\"}}]}}}";
        String responseKnows3=  postQuery(BASE_URL+"personknowsperson/_doc/_search",queryknows3);

        JSONObject jsonObjectKnows3 = new JSONObject(responseKnows3);
        int returntab2= jsonObjectKnows3.getJSONObject("hits").getJSONArray("hits").length();

        for (int j=0; j<returntab2;j++) {
            try {
                Long personid1 = jsonObjectKnows3.getJSONObject("hits").getJSONArray("hits").getJSONObject(j).getJSONObject("_source").getLong("personId1");
                Long personid2 = jsonObjectKnows3.getJSONObject("hits").getJSONArray("hits").getJSONObject(j).getJSONObject("_source").getLong("personId2");
                if (personid1.toString().equals(commonFriends2.get(i))) {}//commonFriends.add(personid1.toString());
                else commonFriends3.add(personid1.toString());
                if (personid2.toString().equals(commonFriends2.get(i))) {}//commonFriends.add(personid1.toString());
                else commonFriends3.add(personid2.toString());

                LinkedHashSet<String> hashSet = new LinkedHashSet<>(commonFriends3);
                commonFriends3 = new ArrayList(hashSet);


            }catch (Exception e){}

        }
        String str="";

    }

    /********on merge les 3 liste et on ne garde que les duplications*/


    for (int dupl1=0; dupl1< commonFriends.size(); dupl1++){
        for (int dupl2=1; dupl2< commonFriends2.size(); dupl2++){
            if (commonFriends.get(dupl1).equals(commonFriends2.get(dupl2))) duplicate.add(commonFriends.get(dupl1));
        }
    }

    for (int dupl1=0; dupl1< commonFriends.size(); dupl1++){
        for (int dupl2=1; dupl2< commonFriends3.size(); dupl2++){
            if (commonFriends.get(dupl1).equals(commonFriends3.get(dupl2))) duplicate.add(commonFriends.get(dupl1));
        }
    }

    for (int dupl1=0; dupl1< commonFriends2.size(); dupl1++){
        for (int dupl2=1; dupl2< commonFriends3.size(); dupl2++){
            if (commonFriends2.get(dupl1).equals(commonFriends3.get(dupl2))) duplicate.add(commonFriends2.get(dupl1));
        }
    }

     LinkedHashSet<String> hashSet = new LinkedHashSet<>(duplicate);
     duplicate = new ArrayList(hashSet);




String str="";
return duplicate;


}



}
