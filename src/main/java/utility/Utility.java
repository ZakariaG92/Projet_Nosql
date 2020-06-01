package utility;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;

import com.google.gson.Gson;
import com.opencsv.CSVReader;

import data.Feedback;
import data.Person;
import data.Post;
import data.order.Order;
import data.product.BrandByProduct;
import data.product.Product;
import data.socialNetwork.PersonKnowsPerson;
import data.vendor.Vendor;
import feedback.Feedbacks;
import okhttp3.OkHttpClient;

public class Utility {

	private static String BASE_URL = "https://c0c2020a12d44546a0a25129e7a11177.europe-west3.gcp.cloud.es.io:9243/";

	/**
	 * Elasticsearch :
	 * @param urlString l'index a fetcher (index/_doc, index/_search ...)
	 * @param method (GET, POST, PUT, DELETE)
	 * @object si objet, la methode se charge a creer le Json equivalant
	 * @objectString si String, la methode passe le directement
	 */
	public static String elasticsearch(String urlString, String method, Object object, String objectString)
			throws IOException {
		Gson gson = new Gson();
		URL url = new URL(BASE_URL + urlString);
		HttpURLConnection con = (HttpURLConnection) url.openConnection();
		con.setRequestMethod(method);
		con.setRequestProperty("Authorization", "Basic ZWxhc3RpYzpuNWZ1NzN0cVlOMVlmVHBNSU16akVlMXI=");
		con.setRequestProperty("Content-Type", "application/json; utf-8");
		con.setRequestProperty("Accept", "application/json");

		con.setDoOutput(true);

		if (method != "GET") {
			String jsonInputString = "";
			if (object != null) {
				jsonInputString = gson.toJson(object);
			} else if (objectString != null) {
				jsonInputString = objectString;
			}

			try (OutputStream os = con.getOutputStream()) {
				byte[] input = jsonInputString.getBytes("utf-8");
				os.write(input, 0, input.length);
			}
		}

		try (BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), "utf-8"))) {
			StringBuilder response = new StringBuilder();
			String responseLine = null;
			while ((responseLine = br.readLine()) != null) {
				response.append(responseLine.trim());
			}

			br.close();

			return response.toString();
		}
	}
	
	
	
	// ***************************** //
	// ***** ADD DELETE UPDATE ***** //
	// ***************************** //
	
	/**	
	* 	@author Zakaria Gasmi
	* 	@param index l'index pour l'insertion des données
	*  	@param body un tableau contenant des contenant des enregistrement, si on souhaite inserer un enregistrement,
	* 	on passe un tableau avec un seul body, sinon on passe un tableau de plusieurs body
	* 	la methode permet une insertion unique ou multiple selon le paramétre donnée
	*/

	public static void insertData(String index, String[] body) throws IOException {

		for (int i = 0; i < body.length; i++) {

			try {

				String response = elasticsearch(index + "/_doc/", "POST", null, body[i]);
				JSONObject jsonObject = new JSONObject(response);

				if (jsonObject.getString("result").equals("created"))
					System.out.println(
							"l'enregistrement avec identifiant " + jsonObject.getString("_id") + " à bien été créer");

			} catch (Exception e) {

				System.out.println("une erreur c'est produite lors de cet création");
			}

		}

	}

	/** 
	 * @author Zakaria Gasmi
	 * delete data est une methode qui en passant l'index et un tableau d'indentifiant en paramétres
	 * permet de supprimer ces enregistrement, on peut mettre un identifiant ou bien plusieurs
	 */

	public static void deleteData(String index, String[] id) throws IOException {
		for (int i = 0; i < id.length; i++) {
			try {

				String responseString = elasticsearch(index + "/_doc/" + id[i], "DELETE", null, null);

				JSONObject jsonObject = new JSONObject(responseString);

				if (jsonObject.getString("result").equals("deleted"))
					System.out.println("l'enregistrement dont l'identifiant " + id[i] + "  à bien été supprimé");

			} catch (Exception e) {
				System.out.println("ERREUR ! l'enregistrement dont l'identifiant " + id[i] + "  est déja supprimé");
			}
		}
	}

	public static void updateField(String index, String field, String value) {
		String query = "{\r\n" + "    \"script\" : \"ctx._source.%s = '%s'\"}";

		try {
			elasticsearch(index + "/_update", "POST", null, String.format(query, field, value));
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void updateDocument(String index, String id, String doc) {
		String query = "{ \"doc\" : %s}";
		try {
			elasticsearch(index + "/_update/" + id, "POST", null, String.format(query, doc));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// ***************************** //
	// *********  LOADERS  ********* //
	// ***************************** //
	
	
	public static void loadCustomerCsv() throws IOException {
		Gson gson = new Gson();

		List<String[]> records = new ArrayList<>();
		try (CSVReader csvReader = new CSVReader(new FileReader("src/main/resources/person.csv"))) {

			for (int i = 0; i <= csvReader.getLinesRead(); i++) {

				records.add(csvReader.readNext());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (int j = 0; j < records.size() - 2; j++) {

			LocalDateTime dateTime = LocalDateTime.parse(records.get(j + 1)[5].substring(0, 19));
			Timestamp timestamp = Timestamp.valueOf(dateTime);
			String time = timestamp.toString();

			Person person = new Person();
			person.id = records.get(j + 1)[0];
			person.firstName = records.get(j + 1)[1];
			person.lastName = records.get(j + 1)[2];
			person.gender = records.get(j + 1)[3];
			person.birthday = records.get(j + 1)[4];
			person.creationDate = time;
			person.locationIP = records.get(j + 1)[6];
			person.browserUsed = records.get(j + 1)[7];
			person.place = Integer.parseInt(records.get(j + 1)[8]);

			elasticsearch("person/_doc/" + person.id,"POST",  person, null);

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
			String json = responseLine.trim().toString();
			Order order = gson.fromJson(json, Order.class);
			try {
				elasticsearch("order/_doc/" + order.orderId.toString(), "POST", order, null);
				//Thread.sleep(1000);
			} catch (Exception e) {
				System.out.println(BASE_URL + "order/_doc/" + order.orderId.toString() + " not created");
			}
		}

	}

	public static void loadInvoices(String path, String name) {
		try {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(path));

			JSONObject xmlJSONObj = XML.toJSONObject(bufferedReader);
			JSONObject p = (JSONObject) xmlJSONObj.get("Invoices");

			JSONArray invoices = p.getJSONArray("Invoice.xml");

			System.out.println(invoices.length());
			Iterator itt = invoices.iterator();

			int i = 0;
			while (itt.hasNext()) {
				JSONObject t = (JSONObject) itt.next();
				try {
					elasticsearch(name + "/_doc/" + i, "POST", null, t.toString());
					System.out.println(i);
					i++;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}

	public static void loadFeedback(String path) {
		try {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
			Scanner sc = new Scanner(bufferedReader);
			int i = 0;
			while (sc.hasNext()) {

				String line = sc.nextLine();

				if (i >= 150000 && i < 160000) {
					String[] trimmed = line.split(Pattern.quote("|"));
					String[] third = trimmed[2].split(",", 2);

					String noteS = third[0];

					noteS = noteS.replaceAll("\"", "");
					noteS = noteS.replaceAll("'", "");
					double note = 2.5;

					third[1] = third[1].replaceAll("\"", "");
					third[1] = third[1].replaceAll("'", "");
					third[1] = third[1].replaceAll("\\\\", "");

					trimmed[0] = trimmed[0].replaceAll("\"", "");
					trimmed[0] = trimmed[0].replaceAll("'", "");

					try {
						note = Double.parseDouble(noteS);
					} catch (Exception e) {
						note = 2.5;
					}

					Feedback toSend = new Feedback(trimmed[0], trimmed[1], note, third[1]);

					try {
						elasticsearch("feedbacks/_doc/" + i, "POST", null, toSend.toJSON());
					} catch (Exception e) {
						try {
							TimeUnit.SECONDS.sleep(10);
						} catch (Exception e1) {
							System.out.println("TimeUnit exception");
						}

						try {
							elasticsearch("feedbacks/_doc/" + i, "POST", null, toSend.toJSON());
						} catch (Exception e2) {
							System.out.println("ERROR " + toSend.toJSON());
						}

					}

					System.out.println(i);
				}

				i++;
			}

			bufferedReader.close();
			sc.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public static void loadPersonHasInterestTag(String path) {
		try {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
			Scanner sc = new Scanner(bufferedReader);

			int i = 0;
			sc.nextLine();
			while (sc.hasNext()) {
				String[] trimmed = sc.nextLine().split(Pattern.quote("|"));
				String json = "{\"personId\":\"" + trimmed[0] + "\", \"tagId\" : \"" + trimmed[1] + "\"}";

				elasticsearch("persontags/_doc/" + i, "POST", null, json);
				i++;
			}

			bufferedReader.close();
			sc.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void loadPosts(String path) {
		try {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
			Scanner sc = new Scanner(bufferedReader);

			int i = 0;
			Gson gson = new Gson();
			String json;
			sc.nextLine();
			String[] trimmed;
			String line;
			while (sc.hasNext()) {
				line = sc.nextLine();
				trimmed = line.split(",");
				int length;
				try {
					length = Integer.parseInt(trimmed[7]);
				} catch (Exception e) {
					length = 0;
				}
				Post post = new Post(trimmed[0], trimmed[1], trimmed[2], trimmed[3], trimmed[4], trimmed[5], trimmed[6],
						length);
				json = gson.toJson(post);
				elasticsearch("posts/_doc/" + post.id, "POST", null, json);
				System.out.println(i);

				i++;
			}
			bufferedReader.close();
			sc.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void addPersonToPosts(String path) {
		try {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
			Scanner sc = new Scanner(bufferedReader);

			int i = 0;
			Gson gson = new Gson();
			String json = "";
			sc.nextLine();
			String[] trimmed;
			String postId = "";
			String personId = "";
		
			while (sc.hasNext()) {
				trimmed = sc.nextLine().split(Pattern.quote("|"));
			
				
				if(i >650000 && i < 8500000 ) {
				try {
					postId = trimmed[0];
					personId = trimmed[1];

					json = "{\"script\" : \"ctx._source.personId = '" + personId + "'\"}";

					elasticsearch("posts/_update/" + postId.replaceAll(" ", ""), "POST", null, json);

				} catch (Exception e) {
					System.out.println("POST posts/_update/" + postId);
					System.out.println(json);

				}
				}
				i++;

			}
			bufferedReader.close();
			sc.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	/*********************products***************/

	public static void loadProduct(String path) throws IOException {

		Gson gson = new Gson();

		List<String[]> products = new ArrayList<>();
		try (CSVReader csvReader = new CSVReader(new FileReader(path))) {

			for (int i = 0; i <= csvReader.getLinesRead(); i++) {
				products.add(csvReader.readNext());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (int j = 0; j < products.size() - 2; j++) {

			Product product = new Product();
			product.asin = products.get(j + 1)[0];
			product.title = products.get(j + 1)[1];
			product.price = Float.parseFloat(products.get(j + 1)[2]);
			product.imgUrl = products.get(j + 1)[3];

			elasticsearch("product/_doc/" + product.asin, "POST", product, null);

		}
	}

	/*****************BrandByProduct************/
	public static void loadBrandByProduct(String path) throws IOException {

		Gson gson = new Gson();

		List<String[]> products = new ArrayList<>();
		try (CSVReader csvReader = new CSVReader(new FileReader(path))) {

			for (int i = 0; i <= csvReader.getLinesRead(); i++) {
				products.add(csvReader.readNext());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (int j = 0; j < products.size() - 1; j++) {

			BrandByProduct brandByProduct = new BrandByProduct();
			brandByProduct.brand = products.get(j)[0];
			brandByProduct.asin = products.get(j)[1];

			elasticsearch("brandbyproduct/_doc/" + brandByProduct.asin, "POST", brandByProduct, null);

		}
	}

	/*****************Vendor************/

	public static void loadVendor(String path) throws IOException {

		Gson gson = new Gson();

		List<String[]> vendors = new ArrayList<>();
		try (CSVReader csvReader = new CSVReader(new FileReader(path))) {

			for (int i = 0; i <= csvReader.getLinesRead(); i++) {
				vendors.add(csvReader.readNext());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (int j = 0; j < vendors.size() - 2; j++) {

			Vendor vendor = new Vendor();
			vendor.vendor = vendors.get(j + 1)[0];
			vendor.country = vendors.get(j + 1)[1];
			vendor.industry = vendors.get(j + 1)[2];

			elasticsearch("vendor/_doc/" + vendor.vendor, "POST", vendor, null);

		}

	}

	/**************************Social Network***************/

	/**************************PersonKnowsPerson***************************/

	public static void loadPersonKnowsPerson(String path) throws IOException {

		Gson gson = new Gson();

		List<String[]> personsConnexion = new ArrayList<>();
		try (CSVReader csvReader = new CSVReader(new FileReader(path))) {

			for (int i = 0; i <= csvReader.getLinesRead(); i++) {
				personsConnexion.add(csvReader.readNext());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (int j = 0; j < personsConnexion.size() - 2; j++) {

			LocalDateTime dateTime = LocalDateTime.parse(personsConnexion.get(j + 1)[2].substring(0, 19));
			Timestamp timestamp = Timestamp.valueOf(dateTime);

			PersonKnowsPerson personKnowsPerson = new PersonKnowsPerson();
			personKnowsPerson.personId1 = Long.parseLong(personsConnexion.get(j + 1)[0]);
			personKnowsPerson.personId2 = Long.parseLong(personsConnexion.get(j + 1)[1]);
			personKnowsPerson.creationDate = timestamp.toString();

			elasticsearch("personknowsperson/_doc", "POST", personKnowsPerson, null);

		}

	}

	// ***************************** //
	// ********* REQUETES ********** //
	// ***************************** //
	
	
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

		String responseProfile = elasticsearch("person/_doc/_search","POST", null, queryProfile);
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

		String responseCommande = elasticsearch("order/_doc/_search", "POST", null,queryCommande);
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

		String responseFeedback = elasticsearch("feedbacks/_doc/_search", "POST", null, queryFeedback);
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

		String responsePeriode= elasticsearch("order/_search", "POST", null, queryPeriode);
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

		String response= elasticsearch("feedbacks/_search", "POST", null, query);
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

		String responsePeriode= elasticsearch("order/_search", "POST", null, queryPeriode);
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

		String response= elasticsearch("feedbacks/_search", "POST", null, query);
		JSONObject jsonObject = new JSONObject(response);
		int len= jsonObject.getJSONObject("hits").getJSONArray("hits").length();

		for (int i=0; i<len; i++)
		{
			// Toutes les personnes qui ont fait un FeedBack pendant toutes les periodes
			String feedback = jsonObject.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source").getString("personId");
			
			// ICI : on peut directement ajouter les notes < 3 mais il va ajouter pour toutes les periodes 
			// alors on passe pour faire des tests

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
		// String response= postQuery(BASE_URL+"order/_doc/_search",query);

		String response= elasticsearch("order/_doc/_search", "POST", null, query);
		
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
			String response1=  elasticsearch("order/_doc/_search", "POST", null ,query1);
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
			String responseKnows=  elasticsearch("personknowsperson/_doc/_search", "POST", null, queryknows);

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
			String responseKnows2=  elasticsearch("personknowsperson/_doc/_search", "POST", null, queryknows2);

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
			String responseKnows3=  elasticsearch("personknowsperson/_doc/_search", "POST", null, queryknows3);

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

	public static String query5(String personId, String category) throws IOException {
		/***Author: Zakaria Gasmi***/
		ArrayList<String> brands = new ArrayList<>();
		ArrayList<String> products = new ArrayList<>();
		ArrayList<String> commonFriends = new ArrayList<>();
		ArrayList<String> commonFriends2 = new ArrayList<>();
		ArrayList<String> commonFriends3 = new ArrayList<>();
		ArrayList<Feedbacks> feedbacksAll = new ArrayList<>();

		/*****on cherche les marques de cette catÃ©gorie*/
		String queryCategory = "{\"size\":100,\"query\":{\"bool\":{\"should\":[{\"term\":{\"industry.keyword\":\""
				+ category + "\"}}]}}}";

		String responseCategory = elasticsearch("vendor/_doc/_search", "POST", null, queryCategory);

		JSONObject jsonObjectCategory = new JSONObject(responseCategory);
		int returntabCategory = jsonObjectCategory.getJSONObject("hits").getJSONArray("hits").length();

		for (int i = 0; i < returntabCategory; i++) {
			String cat = jsonObjectCategory.getJSONObject("hits").getJSONArray("hits").getJSONObject(i)
					.getJSONObject("_source").getString("industry");
			if (cat.equals(category)) {
				brands.add(jsonObjectCategory.getJSONObject("hits").getJSONArray("hits").getJSONObject(i)
						.getJSONObject("_source").getString("vendor"));
			}
		}

		/*****on cherche les amis de cette personne*/

		String queryknows = "{\"size\" : 20,\"query\":{\"bool\":{\"should\":[{\"term\":{\"personId1\":\"" + personId
				+ "\"}},{\"term\":{\"personId2\":\"" + personId + "\"}}]}}}";
		String responseKnows = elasticsearch("personknowsperson/_doc/_search", "POST" , null, queryknows);

		JSONObject jsonObjectKnows = new JSONObject(responseKnows);
		int returntab = jsonObjectKnows.getJSONObject("hits").getJSONArray("hits").length();

		for (int j = 0; j < returntab; j++) {
			try {
				Long personid1 = jsonObjectKnows.getJSONObject("hits").getJSONArray("hits").getJSONObject(j)
						.getJSONObject("_source").getLong("personId1");
				Long personid2 = jsonObjectKnows.getJSONObject("hits").getJSONArray("hits").getJSONObject(j)
						.getJSONObject("_source").getLong("personId2");
				if (personid1.toString().equals(personId)) {
				} //commonFriends.add(personid1.toString());
				else
					commonFriends.add(personid1.toString());
				if (personid2.toString().equals(personId)) {
				} //commonFriends.add(personid1.toString());
				else
					commonFriends.add(personid2.toString());

				LinkedHashSet<String> hashSet = new LinkedHashSet<>(commonFriends);
				commonFriends = new ArrayList(hashSet);

			} catch (Exception e) {
			}

		}

		/********on trouve les amis des amis des personnes et on les ajoute sur une liste commonFriends2 sans duplication*/

		for (int i = 0; i < commonFriends.size(); i++) {
			String queryknows2 = "{\"size\" : 20,\"query\":{\"bool\":{\"should\":[{\"term\":{\"personId1\":\""
					+ commonFriends.get(i) + "\"}},{\"term\":{\"personId2\":\"" + commonFriends.get(i) + "\"}}]}}}";
			String responseKnows2 = elasticsearch("personknowsperson/_doc/_search", "POST", null, queryknows2);

			JSONObject jsonObjectKnows2 = new JSONObject(responseKnows2);
			int returntab2 = jsonObjectKnows2.getJSONObject("hits").getJSONArray("hits").length();

			for (int j = 0; j < returntab2; j++) {
				try {
					Long personid1 = jsonObjectKnows2.getJSONObject("hits").getJSONArray("hits").getJSONObject(j)
							.getJSONObject("_source").getLong("personId1");
					Long personid2 = jsonObjectKnows2.getJSONObject("hits").getJSONArray("hits").getJSONObject(j)
							.getJSONObject("_source").getLong("personId2");
					if (personid1.toString().equals(commonFriends.get(i))) {
					} //commonFriends.add(personid1.toString());
					else
						commonFriends2.add(personid1.toString());
					if (personid2.toString().equals(commonFriends.get(i))) {
					} //commonFriends.add(personid1.toString());
					else
						commonFriends2.add(personid2.toString());

					LinkedHashSet<String> hashSet = new LinkedHashSet<>(commonFriends2);
					commonFriends2 = new ArrayList(hashSet);

				} catch (Exception e) {
				}

			}

		}

		/********on trouve les amis des amis des amis des personnes et on les ajoute sur une liste commonFriends3 sans duplication*/

		for (int i = 0; i < commonFriends2.size(); i++) {
			String queryknows3 = "{\"size\" : 10,\"query\":{\"bool\":{\"should\":[{\"term\":{\"personId1\":\""
					+ commonFriends2.get(i) + "\"}},{\"term\":{\"personId2\":\"" + commonFriends2.get(i) + "\"}}]}}}";
			String responseKnows3 = elasticsearch("personknowsperson/_doc/_search", "POST", null, queryknows3);

			JSONObject jsonObjectKnows3 = new JSONObject(responseKnows3);
			int returntab2 = jsonObjectKnows3.getJSONObject("hits").getJSONArray("hits").length();

			for (int j = 0; j < returntab2; j++) {
				try {
					Long personid1 = jsonObjectKnows3.getJSONObject("hits").getJSONArray("hits").getJSONObject(j)
							.getJSONObject("_source").getLong("personId1");
					Long personid2 = jsonObjectKnows3.getJSONObject("hits").getJSONArray("hits").getJSONObject(j)
							.getJSONObject("_source").getLong("personId2");
					if (personid1.toString().equals(commonFriends2.get(i))) {
					} //commonFriends.add(personid1.toString());
					else
						commonFriends3.add(personid1.toString());
					if (personid2.toString().equals(commonFriends2.get(i))) {
					} //commonFriends.add(personid1.toString());
					else
						commonFriends3.add(personid2.toString());

					LinkedHashSet<String> hashSet = new LinkedHashSet<>(commonFriends3);
					commonFriends3 = new ArrayList(hashSet);

				} catch (Exception e) {
				}

			}
			String str = "";

		}

		/******On merge les 3 listes pour avoit tout les amis********/

		commonFriends.addAll(commonFriends2);
		commonFriends.addAll(commonFriends3);

		/******on enlÃ©ve les doublons********/
		LinkedHashSet<String> hashSet = new LinkedHashSet<>(commonFriends);
		commonFriends = new ArrayList(hashSet);

		/******on rÃ©cupÃ©re les produits de chaque amis et on les mets dans une liste products sans doublons********/
		Iterator iterator = commonFriends.iterator();
		while (iterator.hasNext()) {
			String person = iterator.next().toString();

			String queryproductFriends = "{\"size\":100,\"query\":{\"bool\":{\"should\":[{\"term\":{\"PersonId\":\""
					+ person + "\"}}]}}}";
			String responseProduct = elasticsearch("invoice/_doc/_search", "POST", null, queryproductFriends);

			JSONObject jsonObjectProduct = new JSONObject(responseProduct);
			int returntabProduct = jsonObjectProduct.getJSONObject("hits").getJSONArray("hits").length();

			for (int i = 0; i < returntabProduct; i++) {

				try {
					int returntabProduct2 = jsonObjectProduct.getJSONObject("hits").getJSONArray("hits")
							.getJSONObject(i).getJSONObject("_source").getJSONArray("Orderline").length();

					for (int j = 0; j < returntabProduct2; j++) {
						String brand = jsonObjectProduct.getJSONObject("hits").getJSONArray("hits").getJSONObject(i)
								.getJSONObject("_source").getJSONArray("Orderline").getJSONObject(j).getString("brand");
						String asin = jsonObjectProduct.getJSONObject("hits").getJSONArray("hits").getJSONObject(i)
								.getJSONObject("_source").getJSONArray("Orderline").getJSONObject(j).getString("asin");

						if ((brands.contains(brand) == true) && (products.contains(asin) == false)) {
							products.add(asin);
						}

					}

				} catch (Exception e) {
					String brand = jsonObjectProduct.getJSONObject("hits").getJSONArray("hits").getJSONObject(i)
							.getJSONObject("_source").getJSONObject("Orderline").getString("brand");
					String asin = jsonObjectProduct.getJSONObject("hits").getJSONArray("hits").getJSONObject(i)
							.getJSONObject("_source").getJSONObject("Orderline").getString("asin");

					if ((brands.contains(brand) == true) && (products.contains(asin) == false)) {
						products.add(asin);
					}
				}

			}

		}
		LinkedHashSet<String> hashSetProducts = new LinkedHashSet<>(products);
		products = new ArrayList(hashSetProducts);

		/**ici on cherche les commentaires avec la note de 5**/

		Iterator productsIterator = products.iterator();
		while (productsIterator.hasNext()) {

			String assin = productsIterator.next().toString();
			String queryNote = "{\"query\":{\"bool\":{\"must\":[{\"match\":{\"assin\":\"" + assin
					+ "\"}},{\"match\":{\"note\":\"5.0\"}}]}}}";
			String responseNote = elasticsearch("feedbacks/_doc/_search","POST", null, queryNote);
			JSONObject jsonNote = new JSONObject(responseNote);

			if ((jsonNote.getJSONObject("hits").getJSONObject("total").getInt("value")) > 0) {
				int tabNoteslen = jsonNote.getJSONObject("hits").getJSONArray("hits").length();
				ArrayList<Feedback> feedbacksArray = new ArrayList<>();
				for (int i = 0; i < tabNoteslen; i++) {
					Feedback feedback = new Feedback();
					feedback.note = jsonNote.getJSONObject("hits").getJSONArray("hits").getJSONObject(i)
							.getJSONObject("_source").getInt("note");
					feedback.feedback = jsonNote.getJSONObject("hits").getJSONArray("hits").getJSONObject(i)
							.getJSONObject("_source").getString("feedback");
					feedbacksArray.add(feedback);

				}
				Feedbacks feedbacks = new Feedbacks();
				feedbacks.assin = assin;
				feedbacks.feedbacks = feedbacksArray;
				feedbacksAll.add(feedbacks);

			}

		}
		Gson gson = new Gson();
		return gson.toJson(feedbacksAll);

	}
	
	/** QUERY 6
	 * @author Moussaoui Mohammed
	 * @param personOne Client numero 1
	 * @param personTwo Client numero 2
	 */
	public static void query6(String personOne, String personTwo) {
		ArrayList<ArrayList<String>> toVisit = new ArrayList<ArrayList<String>>();
		ArrayList<ArrayList<String>> afterItt = new ArrayList<ArrayList<String>>();
		ArrayList<String> visited = new ArrayList<>();
		ArrayList<String> found = new ArrayList<>();

		toVisit.add(new ArrayList<>(Arrays.asList(personOne)));

		String query = "{\"size\" : 4000, \"query\" : { \"bool\" : {" + "\"must\": {\"match\" : {\"%s\": \"%s\" }}"
				+ ", \"must_not\": {\"terms\" : {\"%s\" : %s}}}}}";

		String url = "relations/_search";

		JSONObject returned;

		int i = 0;

		mainLoop: while (toVisit.size() != 0) {

			for (ArrayList<String> list : toVisit) {

				try {
					visited.add("\"" + list.get(list.size() - 1) + "\"");

					for (int j = 0; j < 2; j++) {
						returned = new JSONObject(elasticsearch(url, "POST", null,
								String.format(query, j == 0 ? "personOne" : "personTwo", list.get(list.size() - 1),
										j == 0 ? "personTwo" : "personOne", visited.toString())));

						if (returned.getJSONObject("hits") != null
								&& returned.getJSONObject("hits").getJSONObject("total").getInt("value") > 0) {

							Iterator itt = returned.getJSONObject("hits").getJSONArray("hits").iterator();

							while (itt.hasNext()) {
								String person = ((JSONObject) itt.next()).getJSONObject("_source")
										.getString(j == 0 ? "personTwo" : "personOne");
								ArrayList<String> toAdd = new ArrayList<String>(list);
								toAdd.add(person);
								if (person.equals(personTwo)) {
									found = toAdd;
									break mainLoop;
								} else {
									afterItt.add(toAdd);
								}
							}
						}
					}

				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			i++;

			if (i > 30) {
				break mainLoop;
			}
			toVisit.clear();
			toVisit.addAll(afterItt);
			afterItt.clear();
		}

		if (found.size() == 0) {
			System.out.println("Aucune relation entre les deux personnes dans la limite de 30 branche");
		} else {
			System.out.println("La relation la plus proche entre les deux personnes est : ");
			
			System.out.print("[");
			int indexPerson = 0;
			for(String idPerson : found) {
				try {
					String personString = elasticsearch("person/_doc/" + idPerson, "GET", null, null);
					JSONObject personObject = new JSONObject(personString).getJSONObject("_source");
					System.out.print("(" + idPerson + " - " + personObject.getString("firstName") + " " + personObject.getString("lastName") + ")");
					
					if(indexPerson != found.size()) {
						System.out.print(" -> ");
					}
					indexPerson++;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			System.out.println("]");
			
			
			String querySellers = "{ \"size\" : 0, \"query\" : { \"terms\" : { \"PersonId\" : %s } },"
					+ " \"aggs\" : { \"sellers\" : { \"nested\" : { \"path\" : \"Orderline\" }, "
					+ "\"aggs\" : { \"brand\" : { \"terms\" : { \"field\" : \"Orderline.brand.keyword\" }, "
					+ "\"aggs\" : { \"pricesum\" : { \"sum\" : { \"field\" : \"Orderline.price\" } }, "
					+ "\"pricesumSort\": { \"bucket_sort\": { \"sort\": [ {\"pricesum\": {\"order\": \"desc\"}} ], "
					+ "\"size\" : 3 } } } } } } } }";
			
			try {
				String sellersResponseString = elasticsearch("order/_search/", "POST", null, String.format(querySellers,found.toString()));
				JSONArray sellersResponse = new JSONObject(sellersResponseString).getJSONObject("aggregations")
						.getJSONObject("sellers").getJSONObject("brand").getJSONArray("buckets");
				Iterator itS = sellersResponse.iterator();
				
				System.out.println("Meilleurs vendeurs pour ces personnes : ");
				while(itS.hasNext()) {
					JSONObject seller = (JSONObject) itS.next();
					System.out.print(seller.getString("key").replaceAll("_", " "));
					double pricesum = seller.getJSONObject("pricesum").getDouble("value");
					System.out.println(String.format(" d'une valeur de %.2f $", pricesum));
				}
				
			} catch (IOException e) {
				System.out.println("Aucun vondeurs trouvé");
			}
		}
	}


	
	/**** requete 7 *****/
	public static String query7(String vendor) throws IOException {

		/***Author Gasmi Zakaria*/

		ArrayList<String> products = new ArrayList<>();
		ArrayList<String> badSales = new ArrayList<>();
		ArrayList<Feedbacks> feedbacksAll = new ArrayList<>();
		/****recupÃ©ration des produits du vendeur**/
		String query = "{\"size\":300,\"query\":{\"bool\":{\"must\":[{\"match\":{\"brand\":\"" + vendor + "\"}}]}}}";

		String response = elasticsearch("brandbyproduct/_doc/_search", "POST", null, query);
		JSONObject jsonObject = new JSONObject(response);
		int len = jsonObject.getJSONObject("hits").getJSONArray("hits").length();
		for (int i = 0; i < len; i++) {
			products.add(jsonObject.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getString("_id"));
		}

		/**** fin de recupÃ©ration des produits du vendeur**/

		/*** calcul vente produits ***/

		for (int i = 0; i < products.size(); i++) {
			String queryFirstTrimestre = "{\"size\":0,\"query\":{\"bool\":{\"must\":[{\"match\":{\"Orderline.asin\":\""
					+ products.get(i)
					+ "\"}}],\"filter\":[{\"range\":{\"OrderDate\":{\"gte\":\"2020-01-01\"}}},{\"range\":{\"OrderDate\":{\"lte\":\"2020-04-01\"}}}]}},\"aggs\":{\"genres\":{\"value_count\":{\"field\":\"Orderline.asin.keyword\"}}}}";
			String querySecondTrimestre = "{\"size\":0,\"query\":{\"bool\":{\"must\":[{\"match\":{\"Orderline.asin\":\""
					+ products.get(i)
					+ "\"}}],\"filter\":[{\"range\":{\"OrderDate\":{\"gte\":\"2020-04-01\"}}},{\"range\":{\"OrderDate\":{\"lte\":\"2020-07-01\"}}}]}},\"aggs\":{\"genres\":{\"value_count\":{\"field\":\"Orderline.asin.keyword\"}}}}";
			
			String responseFirst = elasticsearch("invoices/_doc/_search", "POST", null,  queryFirstTrimestre);
			String responseSecond = elasticsearch("invoices/_doc/_search", "POST", null, querySecondTrimestre);

			JSONObject jsonFirst = new JSONObject(responseFirst);
			JSONObject jsonSecond = new JSONObject(responseSecond);

			if (jsonFirst.getJSONObject("aggregations").getJSONObject("genres").getInt("value") > jsonSecond
					.getJSONObject("aggregations").getJSONObject("genres").getInt("value")) {
				badSales.add(products.get(i));
			}

		}

		Iterator badIterator = badSales.iterator();
		while (badIterator.hasNext()) {
			String assin = badIterator.next().toString();
			String queryNote = "{\"query\":{\"bool\":{\"must\":[{\"match\":{\"assin\":\"" + assin
					+ "\"}}],\"filter\":[{\"range\":{\"note\":{\"lte\":\"3.0\"}}}]}}}";
			String responseNote = elasticsearch("feedbacks/_doc/_search", "POST", null, queryNote);
			JSONObject jsonNote = new JSONObject(responseNote);

			if ((jsonNote.getJSONObject("hits").getJSONObject("total").getInt("value")) > 0) {
				int tabNoteslen = jsonNote.getJSONObject("hits").getJSONArray("hits").length();
				ArrayList<Feedback> feedbacksArray = new ArrayList<>();
				for (int i = 0; i < tabNoteslen; i++) {
					Feedback feedback = new Feedback();
					feedback.note = jsonNote.getJSONObject("hits").getJSONArray("hits").getJSONObject(i)
							.getJSONObject("_source").getInt("note");
					feedback.feedback = jsonNote.getJSONObject("hits").getJSONArray("hits").getJSONObject(i)
							.getJSONObject("_source").getString("feedback");
					feedbacksArray.add(feedback);

				}
				Feedbacks feedbacks = new Feedbacks();
				feedbacks.assin = assin;
				feedbacks.feedbacks = feedbacksArray;
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
		String response=  elasticsearch("vendor/_search", "POST", null, requeteCategorie);

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

			String respons= elasticsearch("brandbyproduct/_search", "POST", null, query);
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

		String responseAnnee= elasticsearch("order/_search","POST", null, queryAnnee);
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
		else
			System.out.println ("Motant total des ventes de la categorie "+category+" en "+annee+" est " +totalPrice.get(0));


		/******* FIN : On va recuperer toutes les ventes de l'annee donnee en parametre (annee) et ensuite recuperer le montant total de ces ventes /*******/


	}
	
	/**
	 * @author Moussaoui Mohammed
	 * @param country
	 * @param count
	 * @param lastPostsNumber
	 */
	public static void query9(String country, int count, int lastPostsNumber) {

		if (count == 0) {
			count = 3;
		}

		if (lastPostsNumber == 0) {
			lastPostsNumber = 1;
		}
		if (country != null) {
			String vendorsOfCountryQuery = "{\"query\": {\"match\": {\"country\": \"%s\"}}}";
			String sellsAmountQuery = "{\"size\": 0, \"aggs\" : {\"orders\" : {" + " \"nested\" :"
					+ "{\"path\" : \"Orderline\"" + "}," + "\"aggs\": {\"nested\" : {"
					+ "\"filter\" : { \"term\": { \"Orderline.brand.keyword\": \"%s\" } },"
					+ "\"aggs\" : { \"sells\" : { \"sum\" : { \"field\" : \"Orderline.price\" } }" + "}}}}}}";

			String getPersonsIdsQuery = "{\"size\":0, \"query\": {\"nested\": {\"path\": \"Orderline\",\"query\": "
					+ "{\"match\":{\"Orderline.brand\": \"%s\"}}}},"
					+ " \"aggs\" : {\"uniqPersons\" :{\"terms\" : { \"field\" : \"PersonId.keyword\", \"size\" : 10000 }}}}";

			String genderQuery = "{\"size\" : 0,\"query\": {\"terms\": {\"id\": %s}},"
					+ "\"aggs\" : {\"female\" : {\"filter\" : { \"term\": { \"gender\": \"female\" }}},"
					+ "\"male\" : {\"filter\" : { \"term\": { \"gender\": \"male\" } }}}}";

			String lastPostsQuery = "{\"size\" :%d,\"sort\" : [{ \"creationDate\" : \"desc\"}],\"query\" : "
					+ "{\"terms\" : { \"personId\" : %s }}}";

			Map<String, Double> sellers = new HashMap();
			try {

				// Search vendors list of the country
				String vendorsResponse = elasticsearch("vendor/_search", "POST", null,
						String.format(vendorsOfCountryQuery, country));
				JSONObject jsonVendors = new JSONObject(vendorsResponse);

				JSONArray vendors = jsonVendors.getJSONObject("hits").getJSONArray("hits");

				if (vendors.length() > 0) {

					// Search best vendors
					Iterator vIt = vendors.iterator();
					JSONObject vendor = null;
					String sellsAmountResponse;
					JSONObject sellsAmountJson;
					while (vIt.hasNext()) {
						try {
							vendor = (JSONObject) vIt.next();
							String vendorName = vendor.getJSONObject("_source").getString("vendor");

							sellsAmountResponse = elasticsearch("order/_search", "POST", null, String
									.format(sellsAmountQuery, vendor.getJSONObject("_source").getString("vendor")));

							sellsAmountJson = new JSONObject(sellsAmountResponse);

							Double sellsAmount = sellsAmountJson.getJSONObject("aggregations").getJSONObject("orders")
									.getJSONObject("nested").getJSONObject("sells").getDouble("value");

							if (sellers.size() < count) {
								sellers.put(vendorName, sellsAmount);
							} else {
								Double min = Collections.min(sellers.values());
								if (sellsAmount > min) {
									for (Entry<String, Double> entry : sellers.entrySet()) {
										if (entry.getValue().equals(min)) {
											sellers.remove(entry.getKey());
											sellers.put(vendorName, sellsAmount);
											break;
										}
									}
								}
							}

						} catch (Exception e) {
							System.out.println(String.format(sellsAmountQuery,
									vendor.getJSONObject("_source").getString("vendor")));
							e.printStackTrace();
						}

					}

					System.out.println("the " + (count == 1 ? "" : count) + " Compan" + (count == 1 ? "y" : "ies")
							+ " with the best sells :");

					for (Entry<String, Double> v : sellers.entrySet()) {
						System.out
								.println(v.getKey().replaceAll("_", " ") + " with " + Math.round(v.getValue()) + " $");

						// Search all persons of a company
						String personsResponse = elasticsearch("order/_search", "POST", null,
								String.format(getPersonsIdsQuery, v.getKey()));

						JSONArray personsJSONList = new JSONObject(personsResponse).getJSONObject("aggregations")
								.getJSONObject("uniqPersons").getJSONArray("buckets");

						ArrayList<String> personsArray = new ArrayList<String>();

						Iterator itP = personsJSONList.iterator();

						while (itP.hasNext()) {
							personsArray.add("\"" + ((JSONObject) itP.next()).getString("key") + "\"");
						}

						//Get gender percentage
						String genderResponse = elasticsearch("person/_search", "POST", null,
								String.format(genderQuery, personsArray.toString()));

						JSONObject genderJson = new JSONObject(genderResponse);

						int male = genderJson.getJSONObject("aggregations").getJSONObject("male").getInt("doc_count");
						int female = genderJson.getJSONObject("aggregations").getJSONObject("female")
								.getInt("doc_count");

						double malePercentage = (double) male / personsArray.size();
						double femalePercentage = (double) female / personsArray.size();

						System.out.println("Clients number : " + personsArray.size());
						System.out
								.println(String.format("Male percentage (%d): %.2f", male, malePercentage * 100) + "%");
						System.out.println(
								String.format("Female percentage (%d) : %.2f", female, femalePercentage * 100) + "%");

						//Search last posts

						String lastPostsString = elasticsearch("posts/_search", "POST", null,
								String.format(lastPostsQuery, lastPostsNumber, personsArray.toString()));

						JSONObject lastPostsJson = new JSONObject(lastPostsString);

						JSONArray posts = lastPostsJson.getJSONObject("hits").getJSONArray("hits");

						System.out.println("Last Posts : ");

						Iterator itp = posts.iterator();
						while (itp.hasNext()) {
							JSONObject post = ((JSONObject) itp.next()).getJSONObject("_source");

							System.out.println("\tid : " + post.getString("id"));
							System.out.println("\tdate : " + post.getString("creationDate"));
							System.out.println("\tperson : " + post.getString("personId"));
							System.out.println("\tbrowser : " + post.getString("browserUsed"));
							if (post.getInt("length") > 0) {
								System.out.println("\tContent : " + post.getString("content"));
							}
							System.out.println();
						}

						System.out.println("------------------------------------");
					}

				} else {
					System.out.println("No company at " + country);
				}

			} catch (IOException e) {
				e.printStackTrace();
			}

		}

	}
	
	/**
	 * @author Moussaoui Mohammed
	 * @param startString
	 * @param endString
	 */
	public static void query10(String startString, String endString) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		format.setLenient(true);
		try {
			try {
				format.parse(endString);
			} catch (ParseException e) {
				endString = format.format(new Date());
			}

			try {
				format.parse(startString);
			} catch (ParseException e) {
				Calendar c = Calendar.getInstance();
				c.setTime(format.parse(endString));
				c.add(Calendar.YEAR, -1);
				startString = format.format(c.getTime());
			}

			String requestPosts = "{\"size\" : 0, \"query\": {\"range\": { \"creationDate\": { \"gte\": \""
					+ startString + "\", \"lte\" : \"" + endString + "\" }}"
					+ "},\"aggs\" : {\"groupBy\" : {\"terms\" : {\"field\" : \"personId.keyword\" }}}}";

			String response = elasticsearch("post/_search", "POST", null, requestPosts);

			JSONObject responseObj = new JSONObject(response);
			JSONArray buckets = responseObj.getJSONObject("aggregations").getJSONObject("groupBy")
					.getJSONArray("buckets");

			if (buckets.isEmpty()) {
				System.out.println("Aucune personne trouvee");
			} else {
				Iterator it = buckets.iterator();
				long diffLong = format.parse(endString).getTime() - format.parse(startString).getTime();
				int diff = (int) (diffLong / (1000 * 60 * 60 * 25));
				while (it.hasNext()) {
					JSONObject object = (JSONObject) it.next();
					String personId = object.getString("key");
					int count = object.getInt("doc_count");

					JSONObject personInformations = new JSONObject(
							elasticsearch("person/_doc/" + personId, "GET", null, null));

					System.out.println("***** Client avec " + count + " posts ******");
					//System.out.println(personInformations.toString());
					if (personInformations.getBoolean("found")) {
						JSONObject person = personInformations.getJSONObject("_source");
						if (person.isEmpty()) {
							System.out.println("Les informations sur le client numero " + personId + " n'existe pas");

						} else {
							System.out.println("Id :" + person.getString("id"));
							System.out.println("Nom : " + person.getString("firstName"));
							System.out.println("Prenom : " + person.getString("lastName"));
							System.out.println("Sexe : " + person.getString("gender"));
							System.out.println("IP : " + person.getString("locationIP"));

							String requestOrders = "{\"query\" : {\"bool\": {\"must\": {\"match\": "
									+ "{\"PersonId\": \"" + personId + "\"}},\"filter\": "
									+ "{ \"range\": {\"OrderDate\": {\"gte\": \"" + startString + "\"" + ",\"lte\" :\""
									+ endString + "\"}}}}}," + "\"sort\" : {\"OrderDate\":\"desc\"},\"size\" : 1,"
									+ "\"aggs\": {\"total\": {\"sum\": {\"field\": \"TotalPrice\" }}}}";

							JSONObject orderInformations = new JSONObject(
									elasticsearch("order/_search/", "POST", null, requestOrders));

							int total = orderInformations.getJSONObject("hits").getJSONObject("total").getInt("value");

							if (total == 0) {
								System.out.println(
										"Aucun Achat dans la periode entre " + startString + " et " + endString);
							} else {
								JSONObject dernierAchat = ((JSONObject) orderInformations.getJSONObject("hits")
										.getJSONArray("hits").get(0)).getJSONObject("_source");

								System.out.println("Recence -- Dernier Achat :");
								System.out.println("	Id : " + dernierAchat.getString("OrderId"));
								System.out.println("	Date : " + dernierAchat.getString("OrderDate"));
								System.out.println("	Somme : " + dernierAchat.getDouble("TotalPrice"));

								System.out.println("Frequence -- ");
								System.out.println("	nombre d'achat : " + total + " achats");
								System.out.println("    nombre de jours : " + diff + " jours");
								double freq = (double) diff / total;
								System.out.println("    frequence : Un achat chaque  " + Math.round(freq) + " jours");

								System.out.println("Montant --");
								System.out.println("	somme d'achats effectuees : " + Math.round(orderInformations
										.getJSONObject("aggregations").getJSONObject("total").getDouble("value")));
							}

							
							String tagsRequest = "{ \"size\" : 1000, \"query\": { \"match\" : { \"personId\" : \"%s\" } } }";
							String feedbacksRequest = "{ \"size\" : 3, \"query\": { \"match\" : { \"personId\" : \"%s\" } } }";
							
							try {
								String tagsResponse = elasticsearch("", "POST", null, String.format(tagsRequest, person.getString("id")));
								JSONArray tags = new JSONObject(tagsResponse).getJSONObject("hits").getJSONArray("hits");
								if(tags.length() > 0) {
									
								} else {
									System.out.println("Aucun tag trouve");
								}
							} catch (Exception e) {
								System.out.println("Aucun tag trouve");
							}
						}

					} else {
						System.out.println("Les informations sur le client numero " + personId + " n'existe pas");

					}

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	
}
