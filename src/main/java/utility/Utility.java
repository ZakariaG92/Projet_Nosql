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
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.opencsv.CSVReader;

import data.Feedback;
import data.Person;
import data.Post;
import data.order.Order;
import okhttp3.OkHttpClient;

public class Utility {

	public static void loadCustomerCsv() throws IOException {
		Gson gson = new Gson();
		final OkHttpClient httpClient = new OkHttpClient();
		List<Person> personList = new ArrayList<>();
		List<String[]> allRecords = new ArrayList<>();

		List<String[]> records = new ArrayList<>();
		try (CSVReader csvReader = new CSVReader(new FileReader("src/main/resources/person.csv"))) {

			for (int i = 0; i <= csvReader.getLinesRead(); i++) {

				records.add(csvReader.readNext());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (int j = 0; j < 2826 - 2; j++) {

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

			JSONObject result = new JSONObject(
					elasticsearch("http://localhost:9200/customer/_doc/" + person.id, "POST", person, null));

			if (result.getString("result").equals("created")) {
				System.out.println("Add " + j);
			} else {
				System.out.println("ERROR " + j);
			}

		}

	}

	public static String elasticsearch(String urlString, String method, Object object, String objectString)
			throws IOException {
		Gson gson = new Gson();
		URL url = new URL(urlString);
		HttpURLConnection con = (HttpURLConnection) url.openConnection();
		con.setRequestMethod(method);
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
			orders.add(order);
		}

		for (int i = 0; i < orders.size(); i++) {
			try {
				elasticsearch("http://localhost:9200/order/_doc/" + orders.get(i).orderId.toString(), "POST",
						orders.get(i), null);
				//Thread.sleep(1000);
			} catch (Exception e) {
				System.out.println(
						"http://localhost:9200/order/_doc/" + orders.get(i).orderId.toString() + " not created");
			}
		}
	}

	public static void loadXml(String path, String name) {
		try {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
			Scanner sc = new Scanner(bufferedReader);
			String xmlText = "";

			while (sc.hasNext()) {
				xmlText += sc.nextLine();
			}

			JSONObject xmlJSONObj = XML.toJSONObject(xmlText);
			String jsonPrettyPrintString = xmlJSONObj.toString(4);
			JsonParser parser = new JsonParser();
			JsonElement jsonTree = parser.parse(jsonPrettyPrintString);

			if (jsonTree.isJsonObject()) {
				JsonObject jsonObject = jsonTree.getAsJsonObject();
				JsonElement p = jsonObject.get("Invoices");
				JsonArray invoices = p.getAsJsonObject().get("Invoice.xml").getAsJsonArray();
				Iterator it = invoices.iterator();

				int i = 1;
				while (it.hasNext()) {
					JsonObject t = (JsonObject) it.next();
					try {
						System.out.println(t.toString());
						elasticsearch("http://localhost:9200/" + name + "/_doc/" + i, "POST", null, t.toString());
						i++;
					} catch (Exception e) {
						e.printStackTrace();
					}
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
				String[] trimmed = sc.nextLine().split(Pattern.quote("|"));
				Feedback toSend = new Feedback(trimmed[0], trimmed[1], trimmed[0]);
				System.out.println(toSend.toJSON());
				elasticsearch("http://localhost:9200/feedback/_doc/" + i, "POST", null, toSend.toJSON());
			}

			bufferedReader.close();
			sc.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void loadPersonKnowsPerson(String path) {
		try {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
			Scanner sc = new Scanner(bufferedReader);

			int i = 0;
			sc.nextLine();
			while (sc.hasNext()) {
				String[] trimmed = sc.nextLine().split(Pattern.quote("|"));
				String json = "{\"personOne\":\"" + trimmed[0] + "\", \"personTwo\" : \"" + trimmed[1] + "\", "
						+ "\"creationDate\":\"" + trimmed[2] + "\"}";

				elasticsearch("http://localhost:9200/relations/_doc/" + i, "POST", null, json);
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
				String json = "{\"person.id\":\"" + trimmed[0] + "\", \"tag.id\" : \"" + trimmed[1] + "\"}";

				elasticsearch("http://localhost:9200/personTag/_doc/" + i, "POST", null, json);
				System.out.println(i);
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
			while (sc.hasNext()) {
				String[] trimmed = sc.nextLine().split(Pattern.quote("|"));

				Post post = new Post(trimmed[0], trimmed[2], trimmed[3], trimmed[4], trimmed[5], trimmed[6],
						Integer.parseInt(trimmed[7]));
				json = gson.toJson(post);
				elasticsearch("http://localhost:9200/post/_doc/" + post.id, "POST", null, json);
				System.out.println(i);
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
			String json;
			sc.nextLine();
			while (sc.hasNext()) {
				String[] trimmed = sc.nextLine().split(Pattern.quote("|"));

				String postId = trimmed[0];
				String personId = trimmed[1];

				json = "{\"script\" : \"ctx._source.personId = '" + personId + "'\"}";
				//System.out.println(json);
				try {
					elasticsearch("http://localhost:9200/post/_update/" + postId, "POST", null, json);

				} catch (IOException e) {
					System.out.println("ERROR : " + postId);
				}
				System.out.println("Post " + postId + " i " + i);
				i++;
			}
			bufferedReader.close();
			sc.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

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

			System.out.println(requestPosts);
			String response = elasticsearch("http://localhost:9200/post/_search", "POST", null, requestPosts);

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
							elasticsearch("http://localhost:9200/customer/_doc/" + personId, "GET", null, null));

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
									elasticsearch("http://localhost:9200/order/_search/", "POST", null, requestOrders));

							int total = orderInformations.getJSONObject("hits").getJSONObject("total").getInt("value");

							if (total == 0) {
								System.out.println(
										"Aucun Achat dans la periode entre " + startString + " et " + endString);
							} else {
								JSONObject dernierAchat = (JSONObject) orderInformations.getJSONObject("hits")
										.getJSONArray("hits").get(0);

								System.out.println("Recence -- Dernier Achat :");
								System.out.println("	Id : " + dernierAchat.getString("dernierAchat"));
								System.out.println("	Date : " + dernierAchat.getString("OrderDate"));
								System.out.println("	Somme : " + dernierAchat.getString("TotalPrice"));

								System.out.println("Frequence -- ");
								System.out.println("	nombre d'achat : " + total + " achats");
								System.out.println("    nombre de jours : " + diff + " jours");
								System.out.println("    frequence : " + total / diff);

								System.out.println("Montant --");
								System.out.println("	somme d'achats effectuees : " + orderInformations
										.getJSONObject("aggregations").getJSONObject("total").getDouble("value"));
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

	public static void query6(String personOne, String personTwo) {
		ArrayList<ArrayList<String>> toVisit = new ArrayList<ArrayList<String>>();
		ArrayList<ArrayList<String>> afterItt = new ArrayList<ArrayList<String>>();
		ArrayList<String> visited = new ArrayList<>();
		ArrayList<String> found = new ArrayList<>();

		toVisit.add(new ArrayList<>(Arrays.asList(personOne)));

		String query = "{\"size\" : 4000, \"query\" : { \"bool\" : {" + "\"must\": {\"match\" : {\"%s\": \"%s\" }}"
				+ ", \"must_not\": {\"terms\" : {\"%s\" : %s}}}}}";

		String url = "http://localhost:9200/relations/_search";

		JSONObject returned;

		int i = 0;

		mainLoop: while (toVisit.size() != 0) {

			for (ArrayList<String> list : toVisit) {

				try {
					visited.add("\"" + list.get(list.size() - 1) + "\"");

					for (int i = 0; i < 2; i++) {
						returned = new JSONObject(elasticsearch(url, "POST", null,
								String.format(query, i == 0 ? "personOne" : "personTwo", list.get(list.size() - 1),
										i == 0 ? "personTwo" : "personOne", visited.toString())));

						if (returned.getJSONObject("hits") != null
								&& returned.getJSONObject("hits").getJSONObject("total").getInt("value") > 0) {

							Iterator itt = returned.getJSONObject("hits").getJSONArray("hits").iterator();

							while (itt.hasNext()) {
								String person = ((JSONObject) itt.next()).getJSONObject("_source")
										.getString("personTwo");
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

		if (found.size() > 0) {
			System.out.println("Aucune relation entre les deux personnes dans la limite de 30 branche");
		} else {
			System.out.println("La relation la plus proche entre les deux personnes est : " + found);
		}
	}

	public static void main(String[] args) {

	}
}
