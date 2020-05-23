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
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;

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

			postElasticsearch("http://localhost:9200/customer/_doc/" + person.id, person, null);

			System.out.println("Add " + j);
		}

	}

	public static void postElasticsearch(String url1, Object object, String objectString) throws IOException {
		Gson gson = new Gson();
		//URL url = new URL("http://localhost:9200/app1/customer/"+person.id);
		URL url = new URL(url1);
		HttpURLConnection con = (HttpURLConnection) url.openConnection();
		con.setRequestMethod("POST");
		con.setRequestProperty("Content-Type", "application/json; utf-8");
		con.setRequestProperty("Accept", "application/json");
		con.setDoOutput(true);

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

		try (BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), "utf-8"))) {
			StringBuilder response = new StringBuilder();
			String responseLine = null;
			while ((responseLine = br.readLine()) != null) {
				response.append(responseLine.trim());
			}
			//System.out.println(response.toString());
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
				postElasticsearch("http://localhost:9200/app9/order/" + orders.get(i).orderId.toString(), orders.get(i),
						null);
				//Thread.sleep(1000);
			} catch (Exception e) {
				System.out.println(
						"http://localhost:9200/app9/order/" + orders.get(i).orderId.toString() + " not created");
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
						postElasticsearch("http://localhost:9200/" + name + "/_doc/" + i, null, t.toString());
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
				postElasticsearch("http://localhost:9200/feedback/_doc/" + i, null, toSend.toJSON());
			}

			bufferedReader.close();
			sc.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void loadPersonHasInterestTag(String path) {
		try {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
			Scanner sc = new Scanner(bufferedReader);

			int i = 0;
			boolean skipFirst = false;
			while (sc.hasNext()) {
				if (!skipFirst) {
					skipFirst = true;
					break;
				}
				String[] trimmed = sc.nextLine().split(Pattern.quote("|"));
				String json = "{\"person.id\":\"" + trimmed[0] + "\", \"tag.id\" : \" +trimmed[0]+ \"}";

				postElasticsearch("http://localhost:9200/personTag/_doc/" + i, null, json);
			}

			bufferedReader.close();
			sc.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
