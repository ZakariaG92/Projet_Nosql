package main;

import java.io.IOException;
import java.text.ParseException;

import utility.Utility;


public class Main {

	public static void main(String[] args) throws IOException, ParseException {

		//Utility.loadPosts("C:\\Users\\Moussaoui\\Desktop\\post_0_0-2.csv");

		//Utility.loadOrder(
		//	"C:\\Users\\Moussaoui\\Documents\\cours-univ-ecole\\S8\\BigData\\PROJET_NOSQL_2019_2020\\DATA\\Order\\Order.json");

		//Utility.addPersonToPosts(
		//		"C:\\Users\\Moussaoui\\Documents\\cours-univ-ecole\\S8\\BigData\\PROJET_NOSQL_2019_2020\\DATA\\SocialNetwork\\post_hasCreator_person_0_0.csv");
		
		//Utility.query10("2010-01-01", "2012-01-01");

		Utility.query6("8796093023726", "8796093027528");

		//Utility.query9("China", 3, 1);
	}
}
