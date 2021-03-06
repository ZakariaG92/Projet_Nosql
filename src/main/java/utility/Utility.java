package utility;

import com.google.gson.Gson;
import com.opencsv.CSVReader;
import data.Person;
import data.order.Order;
import data.product.BrandByProduct;
import data.product.Product;
import data.socialNetwork.PersonKnowsPerson;
import data.vendor.Vendor;
import feedback.Feedback;
import feedback.Feedbacks;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Timestamp;
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



	/**@param index l'index pour l'insertion des données
	 *@param body un tableau contenant des contenant des enregistrement, si on souhaite inserer un enregistrement,
	 * on passe un tableau avec un seul body, sinon on passe un tableau de plusieurs body
	 * la methode permet une insertion unique ou multiple selon le paramétre donnée*/
	public static void insertData(String index, String[] body) throws IOException {

		/*Zakaria Gasmi*/

		for (int i = 0; i <body.length ; i++) {

			Gson gson = new Gson();
			URL url = new URL(BASE_URL+index+"/_doc/");
			HttpURLConnection con = (HttpURLConnection)url.openConnection();
			con.setRequestMethod("POST");
			con.setRequestProperty("Authorization","Basic ZWxhc3RpYzpuNWZ1NzN0cVlOMVlmVHBNSU16akVlMXI=");
			con.setRequestProperty("Content-Type", "application/json; utf-8");
			con.setRequestProperty("Accept", "application/json");
			con.setDoOutput(true);

			try(OutputStream os = con.getOutputStream()) {
				byte[] input = body[i].getBytes("utf-8");
				os.write(input, 0, input.length);
			}

			try {
				try(BufferedReader br = new BufferedReader(
						new InputStreamReader(con.getInputStream(), "utf-8"))) {
					StringBuilder response = new StringBuilder();
					String responseLine = null;
					while ((responseLine = br.readLine()) != null) {
						response.append(responseLine.trim());
					}

					JSONObject jsonObject = new JSONObject(response.toString());


					if (jsonObject.getString("result").equals("created")) System.out.println("l'enregistrement avec identifiant "+jsonObject.getString("_id")+" à bien été créer");
				}

			} catch (Exception e){
				System.out.println("une erreur c'est produite lors de cet création");
			}


		}

	}


	public static void postElasticsearch(String url1,Object object) throws IOException {

		/*Zakaria Gasmi*/
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

	/****delete data est une methode qui en passant l'index et un tableau d'indentifiant en paramétres
	 * permet de supprimer ces enregistrement, on peut mettre un identifiant ou bien plusieurs*/
	public static void deleteData(String index, String[] id) throws IOException {
		/******Zakaria Gasmi*******/
		for (int i = 0; i <id.length ; i++) {

			String responseString;
			URL url = new URL(BASE_URL+index+"/_doc/"+id[i]);
			HttpURLConnection con = (HttpURLConnection)url.openConnection();
			con.setRequestMethod("DELETE");
			con.setRequestProperty("Authorization","Basic ZWxhc3RpYzpuNWZ1NzN0cVlOMVlmVHBNSU16akVlMXI=");
			con.setDoOutput(true);

			try{
				try(BufferedReader br = new BufferedReader(
						new InputStreamReader(con.getInputStream(), "utf-8"))) {
					StringBuilder response = new StringBuilder();
					String responseLine = null;
					while ((responseLine = br.readLine()) != null) {
						response.append(responseLine.trim());
					}

					responseString=response.toString();

					JSONObject jsonObject = new JSONObject(responseString);


					if (jsonObject.getString("result").equals("deleted")) System.out.println("l'enregistrement dont l'identifiant "+id[i]+"  à bien été supprimé");;
					String str="";
				}}
			catch (Exception e){System.out.println("ERREUR ! l'enregistrement dont l'identifiant "+id[i]+"  est déja supprimé");}

		}
	}


	public static String postQuery(String url1,String query) throws IOException {

		/******Zakaria Gasmi*******/
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

		/***Author Zakaria Gasmi***/

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

		return duplicate;


	}

	/**** requete 7 *****/
	public static String query7(String vendor) throws IOException {

		/***Author Gasmi Zakaria*/

		ArrayList<String> products= new ArrayList<>();
		ArrayList<String> badSales= new ArrayList<>();
		ArrayList<Feedbacks> feedbacksAll= new ArrayList<>();
		/****recupération des produits du vendeur**/
		String query= "{\"size\":300,\"query\":{\"bool\":{\"must\":[{\"match\":{\"brand\":\""+vendor+"\"}}]}}}";

		String response= postQuery(BASE_URL+"brandbyproduct/_doc/_search",query);
		JSONObject jsonObject = new JSONObject(response);
		int len= jsonObject.getJSONObject("hits").getJSONArray("hits").length();
		for (int i=0; i<len; i++){ products.add(jsonObject.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getString("_id")); }

		/**** fin de recupération des produits du vendeur**/

		/*** calcul vente produits ***/

		for (int i=0; i<products.size(); i++) {
			String queryFirstTrimestre = "{\"size\":0,\"query\":{\"bool\":{\"must\":[{\"match\":{\"Orderline.asin\":\""+products.get(i)+"\"}}],\"filter\":[{\"range\":{\"OrderDate\":{\"gte\":\"2020-01-01\"}}},{\"range\":{\"OrderDate\":{\"lte\":\"2020-04-01\"}}}]}},\"aggs\":{\"genres\":{\"value_count\":{\"field\":\"Orderline.asin.keyword\"}}}}";
			String querySecondTrimestre = "{\"size\":0,\"query\":{\"bool\":{\"must\":[{\"match\":{\"Orderline.asin\":\""+products.get(i)+"\"}}],\"filter\":[{\"range\":{\"OrderDate\":{\"gte\":\"2020-04-01\"}}},{\"range\":{\"OrderDate\":{\"lte\":\"2020-07-01\"}}}]}},\"aggs\":{\"genres\":{\"value_count\":{\"field\":\"Orderline.asin.keyword\"}}}}";
			String responseFirst= postQuery(BASE_URL+"invoices/_doc/_search",queryFirstTrimestre);
			String responseSecond= postQuery(BASE_URL+"invoices/_doc/_search",querySecondTrimestre);

			JSONObject jsonFirst = new JSONObject(responseFirst);
			JSONObject jsonSecond = new JSONObject(responseSecond);

			if (jsonFirst.getJSONObject("aggregations").getJSONObject("genres").getInt("value")>jsonSecond.getJSONObject("aggregations").getJSONObject("genres").getInt("value")){badSales.add(products.get(i));}

		}

		Iterator badIterator = badSales.iterator();
		while (badIterator.hasNext())
		{
			String assin= badIterator.next().toString();
			String queryNote="{\"query\":{\"bool\":{\"must\":[{\"match\":{\"assin\":\""+assin+"\"}}],\"filter\":[{\"range\":{\"note\":{\"lte\":\"3.0\"}}}]}}}";
			String responseNote= postQuery(BASE_URL+"feedbacks/_doc/_search",queryNote);
			JSONObject jsonNote= new JSONObject(responseNote);

			if ((jsonNote.getJSONObject("hits").getJSONObject("total").getInt("value"))>0)
			{
				int tabNoteslen= jsonNote.getJSONObject("hits").getJSONArray("hits").length();
				ArrayList<Feedback> feedbacksArray= new ArrayList<>();
				for (int i = 0; i <tabNoteslen ; i++) {
					Feedback feedback = new Feedback();
					feedback.note= jsonNote.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getInt("note");
					feedback.feedback=jsonNote.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getString("feedback");
					feedbacksArray.add(feedback);

				}
				Feedbacks feedbacks = new Feedbacks();
				feedbacks.assin=assin;
				feedbacks.feedbacks=feedbacksArray;
				feedbacksAll.add(feedbacks);

			}


		}

		Gson gson= new Gson();
		return gson.toJson(feedbacksAll);

	}
	
	/**
	 * QUERY 2
	 * @author : Mohamed TONZAR
	 * @param dateDebut : la date de debut
	 * @param dateFin : la date de fin
	 * @param asiin : le produit
	 * @throws IOException
	 */
	public static void query2(String dateDebut, String dateFin, String asiin) throws IOException
	{
		// pour stocker les personnes commandee ce produit pendant la periode demendee
		ArrayList<String> personnesPeriode = new ArrayList<>();
		// pour stocker les personnes qui ont fait des commentaires pour ce produit
		ArrayList<String> personnesFeedback = new ArrayList<>();
		

		// pour stocker les commentaires 
		ArrayList<String> commentaires = new ArrayList<>();
		Set<String> setCommentaires = new HashSet<>(commentaires); /*des HashSet pour eviter les doublons*/


		/******* DEBUT :  Recuperer les clients qui ont commande ce produit pendant cette periode *******/

		// On commence par filtrer par la periode
		String queryPeriode= "{\"size\" : 200, \"query\": {\"range\": { \"OrderDate\": { \"gte\": \""+dateDebut+"\", \"lte\" : \""+dateFin+"\" }}}}";

		String responsePeriode= postQuery(BASE_URL+"order/_doc/_search",queryPeriode);
		JSONObject jsonObjectPeriode = new JSONObject(responsePeriode);

		int lenJsonObjectPeriode= jsonObjectPeriode.getJSONObject("hits").getJSONArray("hits").length();

		for (int i=0; i<lenJsonObjectPeriode; i++)
		{
			int lenJsonObjectPeriode2 = jsonObjectPeriode.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getJSONArray("Orderline").length();

			for (int j = 0; j < lenJsonObjectPeriode2 ; j++) {

				// On recupere tous les produits commandee pendant cette periode
				String asin = jsonObjectPeriode.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getJSONArray("Orderline").getJSONObject(j).getString("asin");


				// On compare les produits de la periode avec notre produit demande en parametre
				if (asiin.equals(asin))
				{
					// Si c'est notre produit, on stock ces clients dans la liste 'personnes'
					personnesPeriode.add(jsonObjectPeriode.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getString("PersonId"));

				}
			}

		}

		/******* FIN : Recuperer les clients qui ont commande ce produit pendant cette periode *******/

		/******* DEBUT :  Recuperer les personnes qui ont fait a FeedBack pour ce produit *******/

		String query= "{\"size\":300,\"query\":{\"bool\":{\"must\":[{\"match\":{\"assin\":\""+asiin+"\"}}]}}}";

		String response= postQuery(BASE_URL+"feedbacks/_doc/_search",query);
		JSONObject jsonObject = new JSONObject(response);
		int len= jsonObject.getJSONObject("hits").getJSONArray("hits").length();

		for (int i=0; i<len; i++)
		{
			// Toutes les personnes qui ont fait un FeedBack pendant toutes les periodes
			String feedback = jsonObject.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getString("personId");

			for(String elem: personnesFeedback)
			{
				// Comparer les personnes (entre la periode et le Feedback)
				if (elem.equals(feedback))
				{
					// Ajouter les personnes qui ont fait un FeedBack pendant la periode demandee
					personnesFeedback.add(jsonObject.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getString("personId"));

					System.out.println("\n\n********** Les personnes qui ont commente et acheter le produit "+asiin+" entre le : "+dateDebut+" et "+dateFin+" sont : **********");
					System.out.println ("- "+elem);
			
				}

			}
			
		
			/******* FIN :  Recuperer les personnes qui ont fait un FeedBack et achter pour ce produit *******/

		}
	}


	/**
	 * QUERY 3
	 * @author : Jassim EL HAROUI
	 * @param dateDebut : la date de debut
	 * @param dateFin : la date de fin
	 * @param asiin : le produit
	 * @throws IOException
	 */
	public static void query3(String dateDebut, String dateFin, String asiin) throws IOException
	{
		// pour stocker les personnes commandee ce produit pendant la periode demendee
		ArrayList<String> personnesPeriode = new ArrayList<>();
		// pour stocker les personnes qui ont fait des commentaires pour ce produit
		ArrayList<String> personnesFeedback = new ArrayList<>();
		// pour stocker les notes
		ArrayList<Integer> notes = new ArrayList<>();

		// pour stocker les commentaires 
		ArrayList<String> commentaires = new ArrayList<>();
		Set<String> setCommentaires = new HashSet<>(commentaires); /*des HashSet pour eviter les doublons*/


		/******* DEBUT :  Recuperer les clients qui ont commande ce produit pendant cette periode *******/

		// On commence par filtrer par la periode
		String queryPeriode= "{\"size\" : 200, \"query\": {\"range\": { \"OrderDate\": { \"gte\": \""+dateDebut+"\", \"lte\" : \""+dateFin+"\" }}}}";

		String responsePeriode= postQuery(BASE_URL+"order/_doc/_search",queryPeriode);
		JSONObject jsonObjectPeriode = new JSONObject(responsePeriode);

		int lenJsonObjectPeriode= jsonObjectPeriode.getJSONObject("hits").getJSONArray("hits").length();

		for (int i=0; i<lenJsonObjectPeriode; i++)
		{
			int lenJsonObjectPeriode2 = jsonObjectPeriode.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getJSONArray("Orderline").length();

			for (int j = 0; j < lenJsonObjectPeriode2 ; j++) {

				// On recupere tous les produits commandee pendant cette periode
				String asin = jsonObjectPeriode.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getJSONArray("Orderline").getJSONObject(j).getString("asin");


				// On compare les produits de la periode avec notre produit demande en parametre
				if (asiin.equals(asin))
				{
					// Si c'est notre produit, on stock ces clients dans la liste 'personnes'
					personnesPeriode.add(jsonObjectPeriode.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getString("PersonId"));

				}
			}

		}

		/******* FIN : Recuperer les clients qui ont commande ce produit pendant cette periode *******/

		/******* DEBUT :  Recuperer les personnes qui ont fait un FeedBack negatif pour ce produit *******/

		String query= "{\"size\":300,\"query\":{\"bool\":{\"must\":[{\"match\":{\"assin\":\""+asiin+"\"}}]}}}";

		String response= postQuery(BASE_URL+"feedbacks/_doc/_search",query);
		JSONObject jsonObject = new JSONObject(response);
		int len= jsonObject.getJSONObject("hits").getJSONArray("hits").length();

		for (int i=0; i<len; i++)
		{
			// Toutes les personnes qui ont fait un FeedBack pendant toutes les periodes
			String feedback = jsonObject.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getString("personId");

			for(String elem: personnesFeedback)
			{
				// Comparer les personnes (entre la periode et le Feedback)
				if (elem.equals(feedback))
				{
					// Ajouter les personnes qui ont fait un FeedBack pendant la periode demandee
					personnesFeedback.add(jsonObject.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getString("personId"));

					// Tester si la note est moins de 3
					if (jsonObject.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getInt("note") <= 3)
					{
						// Ajouter les commentaires negatifs
						setCommentaires.add(jsonObject.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getString("feedback"));
					}
				}

			}
		}

		// verifier notre liste pour l'affichage
		if (setCommentaires.isEmpty())
		{
			System.out.println("");
			System.out.println("********** OUPS !! Aucune personnes n'a poster un avis negatif pour ce produit "+asiin+" entre le : "+dateDebut+" et "+dateFin+" **********");

		}
		else
		{
			System.out.println("\n\n********** Les commentaires qui ont moins de 3 etoiles de "+asiin+" entre le : "+dateDebut+" et "+dateFin+" sont : **********");

			for(String elem: setCommentaires)
			{
				System.out.println ("- "+elem);
			}

			/******* FIN :  Recuperer les personnes qui ont fait un FeedBack negatif pour ce produit *******/

		}
	}


	public static String query5(String personId, String category) throws IOException {
		/***Author: Zakaria Gasmi***/
		ArrayList<String> brands= new ArrayList<>();
		ArrayList<String> products= new ArrayList<>();
		ArrayList<String> commonFriends= new ArrayList<>();
		ArrayList<String> commonFriends2= new ArrayList<>();
		ArrayList<String> commonFriends3= new ArrayList<>();
		ArrayList<Feedbacks> feedbacksAll=new ArrayList<>();

		/*****on cherche les marques de cette catégorie*/
		String queryCategory="{\"size\":100,\"query\":{\"bool\":{\"should\":[{\"term\":{\"industry.keyword\":\""+category+"\"}}]}}}";

		String responseCategory=  postQuery(BASE_URL+"vendor/_doc/_search",queryCategory);

		JSONObject jsonObjectCategory = new JSONObject(responseCategory);
		int returntabCategory= jsonObjectCategory.getJSONObject("hits").getJSONArray("hits").length();

		for (int i = 0; i <returntabCategory ; i++) {
			String cat=jsonObjectCategory.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getString("industry");
			if (cat.equals(category))
			{
				brands.add(jsonObjectCategory.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getString("vendor"));
			}
		}


		/*****on cherche les amis de cette personne*/

		String queryknows= "{\"size\" : 20,\"query\":{\"bool\":{\"should\":[{\"term\":{\"personId1\":\""+personId+"\"}},{\"term\":{\"personId2\":\""+personId+"\"}}]}}}";
		String responseKnows=  postQuery(BASE_URL+"personknowsperson/_doc/_search",queryknows);

		JSONObject jsonObjectKnows = new JSONObject(responseKnows);
		int returntab= jsonObjectKnows.getJSONObject("hits").getJSONArray("hits").length();

		for (int j=0; j<returntab;j++) {
			try {
				Long personid1 = jsonObjectKnows.getJSONObject("hits").getJSONArray("hits").getJSONObject(j).getJSONObject("_source").getLong("personId1");
				Long personid2 = jsonObjectKnows.getJSONObject("hits").getJSONArray("hits").getJSONObject(j).getJSONObject("_source").getLong("personId2");
				if (personid1.toString().equals(personId)) {}//commonFriends.add(personid1.toString());
				else commonFriends.add(personid1.toString());
				if (personid2.toString().equals(personId)) {}//commonFriends.add(personid1.toString());
				else commonFriends.add(personid2.toString());

				LinkedHashSet<String> hashSet = new LinkedHashSet<>(commonFriends);
				commonFriends = new ArrayList(hashSet);


			}catch (Exception e){}

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

		/******On merge les 3 listes pour avoit tout les amis********/

		commonFriends.addAll(commonFriends2);
		commonFriends.addAll(commonFriends3);

		/******on enléve les doublons********/
		LinkedHashSet<String> hashSet = new LinkedHashSet<>(commonFriends);
		commonFriends = new ArrayList(hashSet);



		/******on récupére les produits de chaque amis et on les mets dans une liste products sans doublons********/
		Iterator iterator= commonFriends.iterator();
		while (iterator.hasNext()) {
			String person=iterator.next().toString();

			String queryproductFriends = "{\"size\":100,\"query\":{\"bool\":{\"should\":[{\"term\":{\"PersonId\":\""+person+"\"}}]}}}";
			String responseProduct = postQuery(BASE_URL + "invoice/_doc/_search", queryproductFriends);

			JSONObject jsonObjectProduct = new JSONObject(responseProduct);
			int returntabProduct = jsonObjectProduct.getJSONObject("hits").getJSONArray("hits").length();

			for (int i = 0; i <returntabProduct ; i++) {


				try {
					int returntabProduct2 = jsonObjectProduct.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getJSONArray("Orderline").length();

					for (int j = 0; j <returntabProduct2 ; j++) {
						String brand = jsonObjectProduct.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getJSONArray("Orderline").getJSONObject(j).getString("brand");
						String asin = jsonObjectProduct.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getJSONArray("Orderline").getJSONObject(j).getString("asin");

						if ((brands.contains(brand)==true) && (products.contains(asin)==false)){products.add(asin);}

					}

				}catch (Exception e){
					String brand = jsonObjectProduct.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getJSONObject("Orderline").getString("brand");
					String asin = jsonObjectProduct.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getJSONObject("Orderline").getString("asin");

					if ((brands.contains(brand)==true) && (products.contains(asin)==false)){products.add(asin);}
				}



			}

		}
		LinkedHashSet<String> hashSetProducts = new LinkedHashSet<>(products);
		products = new ArrayList(hashSetProducts);

		/**ici on cherche les commentaires avec la note de 5**/

		Iterator productsIterator= products.iterator();
		while (productsIterator.hasNext()){

			String assin= productsIterator.next().toString();
			String queryNote="{\"query\":{\"bool\":{\"must\":[{\"match\":{\"assin\":\""+assin+"\"}},{\"match\":{\"note\":\"5.0\"}}]}}}";
			String responseNote= postQuery(BASE_URL+"feedbacks/_doc/_search",queryNote);
			JSONObject jsonNote= new JSONObject(responseNote);

			if ((jsonNote.getJSONObject("hits").getJSONObject("total").getInt("value"))>0)
			{
				int tabNoteslen= jsonNote.getJSONObject("hits").getJSONArray("hits").length();
				ArrayList<Feedback> feedbacksArray= new ArrayList<>();
				for (int i = 0; i <tabNoteslen ; i++) {
					Feedback feedback = new Feedback();
					feedback.note= jsonNote.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getInt("note");
					feedback.feedback=jsonNote.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getString("feedback");
					feedbacksArray.add(feedback);

				}
				Feedbacks feedbacks = new Feedbacks();
				feedbacks.assin=assin;
				feedbacks.feedbacks=feedbacksArray;
				feedbacksAll.add(feedbacks);

			}

		}
		Gson gson = new Gson();
		return gson.toJson(feedbacksAll);

	}

	/**
	 * QUERY 8
	 * @author : Jassim EL HAROUI
	 * @param category : la categorie  ==> industry dans 'vendor'
	 * @param annee : l'annee
	 * @throws IOException
	 */
	public static void query8(String category, String annee) throws IOException 
	{

		// pour les vendeurs
		ArrayList<String> vendeurs= new ArrayList<>();
		// pour les prodits
		ArrayList<String> produitsCategorie= new ArrayList<>();
		// pour les prix
		ArrayList<Integer> totalPrice= new ArrayList<>();


		/******* DEBUT : On va recuperer tous les vendeurs de cette categorie donnee en parametre /*******/

		String requeteCategorie="{\"size\":200,\"query\":{\"bool\":{\"should\":[{\"term\":{\"industry.keyword\":\""+category+"\"}}]}}}";
		String response=  postQuery(BASE_URL+"vendor/_doc/_search",requeteCategorie);

		JSONObject jsonCategories = new JSONObject(response);
		int lengthJsonCategories= jsonCategories.getJSONObject("hits").getJSONArray("hits").length();

		for (int i = 0; i <lengthJsonCategories ; i++) {

			String categorie=jsonCategories.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getString("industry");

			// si c'est bien la categorie demandee, on stock leurs vendeurs dans la liste vendeurs
			if (categorie.equals(category))
			{
				vendeurs.add(jsonCategories.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getString("vendor"));
			}
		}

		/******* FIN : On va recuperer tous les vendeurs de cette categorie donnee en parametre /*******/

		/******* DEBUT : On va recuperer tous les produits de ces vendeurs /*******/

		for(String vend: vendeurs)
		{

			String query= "{\"size\":300,\"query\":{\"bool\":{\"must\":[{\"match\":{\"brand\":\""+vend+"\"}}]}}}";

			String respons= postQuery(BASE_URL+"brandbyproduct/_doc/_search",query);
			JSONObject jsonProducts = new JSONObject(respons);

			int lengthJsonProducts= jsonProducts.getJSONObject("hits").getJSONArray("hits").length();
			for (int i=0; i<lengthJsonProducts; i++)
			{ 
				// Ajout dans la liste produits
				produitsCategorie.add(jsonProducts.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getString("_id")); 
			}

		}

		/*for(String elem: produits)
        {
            System.out.println ("ASIN : "+elem);
        }*/

		/******* FIN : On va recuperer tous les produits de ces vendeurs /*******/


		/******* DEBUT : On va recuperer toutes les ventes de l'annee donnee en parametre (annee) et ensuite recuperer le montant total de ces ventes /*******/

		String queryAnnee= "{\"size\":300,\"query\":{\"bool\":{\"must\":[{\"match\":{\"OrderDate\":\""+annee+"\"}}]}}}";

		String responseAnnee= postQuery(BASE_URL+"order/_doc/_search",queryAnnee);
		JSONObject jsonAnnee = new JSONObject(responseAnnee);

		int lengthJsonAnnee= jsonAnnee.getJSONObject("hits").getJSONArray("hits").length();

		for (int i=0; i<lengthJsonAnnee; i++)
		{ 
			// On va recuperer le length de chaque "Orderline"
			int lengthJsonAnnee2 = jsonAnnee.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getJSONArray("Orderline").length();

			for (int j=0; j < lengthJsonAnnee2; j++)
			{
				// On va recuperer tous les produit vendus cette annee
				String asinAnnee = jsonAnnee.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getJSONArray("Orderline").getJSONObject(j).getString("asin");

				// asinAnnee                 : on prend les produits vendus cette annee
				// liste 'produitsCategorie' : on prend les produits de la categorie
				// On va voir si ils ont les deux criteres demandee en meme temps (annee et categorie)
				// Pour cela on va faire une comparaison
				for(String elem: produitsCategorie)
				{
					// On compare et ajoute dans la liste "totalprice"
					if (elem == asinAnnee)
						totalPrice.add(jsonAnnee.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getInt("TotalPrice"));     		
				}
			}

		}

		if (totalPrice.isEmpty())
			System.out.println("OUPS !! Cette categorie : "+category+" n'a fait aucune vente en "+annee);

		/*for(int elem: totalPrice)
		{
			System.out.println ("Motant total - "+elem);
		}*/

		/******* FIN : On va recuperer toutes les ventes de l'annee donnee en parametre (annee) et ensuite recuperer le montant total de ces ventes /*******/


	}


	/**
	 * QUERY 1
	 * @author : Jassim EL HAROUI
	 * @param personne : le client donne
	 * @throws IOException
	 */
	public static void query1(String personne) throws IOException 
	{

		// pour ses commandes
		ArrayList<String> commandes = new ArrayList<>();
		// pour ses commentaires
		ArrayList<String> feedbacks = new ArrayList<>();

		/******* DEBUT : Trouver le profile du client souhaite /*******/

		String queryProfile = "{\"size\":300,\"query\":{\"bool\":{\"must\":[{\"match\":{\"_id\":\""+personne+"\"}}]}}}";

		String responseProfile = postQuery(BASE_URL+"person/_doc/_search",queryProfile);
		JSONObject jsonProfile = new JSONObject(responseProfile);
		//System.out.println(jsonProfile.toString());

		// extraire son profile
		String prenom = jsonProfile.getJSONObject("hits").getJSONArray("hits").getJSONObject(0).getJSONObject("_source").getString("firstName");		
		String nom = jsonProfile.getJSONObject("hits").getJSONArray("hits").getJSONObject(0).getJSONObject("_source").getString("lastName");	
		String gender = jsonProfile.getJSONObject("hits").getJSONArray("hits").getJSONObject(0).getJSONObject("_source").getString("gender");

		System.out.println(" ");
		System.out.println("********* Le profile du client  : "+personne+" *********");
		System.out.println(" ==> sexe : "+gender+", Prenom : "+prenom+", Nom : "+nom);
		System.out.println(" ");

		/******* FIN : Trouver le profile du client souhaite /*******/

		/******* DEBUT : Trouver ses commandes et factres /*******/

		String queryCommande = "{\"size\":300,\"query\":{\"bool\":{\"must\":[{\"match\":{\"PersonId\":\""+personne+"\"}}]}}}";

		String responseCommande = postQuery(BASE_URL+"order/_doc/_search",queryCommande);
		JSONObject jsonCommande = new JSONObject(responseCommande);
		//System.out.println(jsonCommande.toString());

		// extraire ses commandes
		int lengthJsoCommandes= jsonCommande.getJSONObject("hits").getJSONArray("hits").length();

		for (int i=0; i<lengthJsoCommandes; i++)
		{ 
			commandes.add(jsonCommande.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getString("OrderId"));     		
		}

		System.out.println(" ");
		System.out.println("********* Il a fait les commandes N : *********");
		for(String elem: commandes)
		{
			System.out.print (" // "+elem);
		}	

		System.out.println(" ");
		System.out.println(" ");
		System.out.println("********* Il a les factures N : *********");
		for(String elem: commandes)
		{
			System.out.print (" // "+elem);
		}	

		/******* FIN : Trouver ses commandes et factres /*******/

		/******* DEBUT : Trouver ses commentaires /*******/

		String queryFeedback= "{\"size\":100,\"query\":{\"bool\":{\"must\":[{\"match\":{\"personId\":\""+personne+"\"}}]}}}";

		String responseFeedback = postQuery(BASE_URL+"feedbacks/_doc/_search",queryFeedback);
		JSONObject jsonFeedback = new JSONObject(responseFeedback);
		//System.out.println(jsonFeedback.toString());

		// extraire ses commentaires
		int lengthJsoFeedback= jsonFeedback.getJSONObject("hits").getJSONArray("hits").length();

		for (int i=0; i<lengthJsoFeedback; i++)
		{ 
			feedbacks.add(jsonFeedback.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getString("feedback"));     		
		}

		System.out.println(" ");
		System.out.println("********* Il a fait ces commentaires : *********");
		for(String elem: feedbacks)
		{
			System.out.println (" // "+elem);
		}	


		/******* FIN : Trouver ses commentaires /*******/



	}


}
