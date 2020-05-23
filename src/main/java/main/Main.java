package main;

import java.io.IOException;
import java.text.ParseException;

import utility.Utility;;

public class Main {

	public static void main(String[] args) throws IOException, ParseException {
		/*Utility.loadXml(
				"C:\\Users\\Moussaoui\\Documents\\cours-univ-ecole\\S8\\BigData\\PROJET_NOSQL_2019_2020\\DATA\\Invoice\\Invoice-1.xml",
				"test");*/

		/*Utility.loadfeedBack(
				"C:\\Users\\Moussaoui\\Documents\\cours-univ-ecole\\S8\\BigData\\PROJET_NOSQL_2019_2020\\DATA\\Feedback\\Feedback - Copie.csv");*/

		/*Utility.loadPersonHasInterestTag(
				"C:\\Users\\Moussaoui\\Documents\\cours-univ-ecole\\S8\\BigData\\PROJET_NOSQL_2019_2020\\DATA\\SocialNetwork\\person_hasInterest_tag_0_0.csv");*/

		Utility.loadPosts(
				"C:\\Users\\Moussaoui\\Documents\\cours-univ-ecole\\S8\\BigData\\PROJET_NOSQL_2019_2020\\DATA\\SocialNetwork\\post_0_0.csv");

	}

}
