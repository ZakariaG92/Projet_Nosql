package main;

import java.io.IOException;
import java.text.ParseException;

import utility.Utility;;

public class Main {

	public static void main(String[] args) throws IOException, ParseException {
		//Utility.loadCustomerCsv();

		//Utility.loadPosts(
		//		"C:\\Users\\Moussaoui\\Documents\\cours-univ-ecole\\S8\\BigData\\PROJET_NOSQL_2019_2020\\DATA\\SocialNetwork\\post_0_0.csv");

		//Utility.addPersonToPosts(
		//		"C:\\Users\\Moussaoui\\Documents\\cours-univ-ecole\\S8\\BigData\\PROJET_NOSQL_2019_2020\\DATA\\SocialNetwork\\post_hasCreator_person_0_0.csv");

		Utility.searchPosts("2010-01-01", "2012-01-01");
	}

	/*
	{
	  "size" : 0,
	  "query": { 
	    "bool": { 
	      "filter": [ 
	        { "range": { "creationDate": { "gte": "2012-10-14" }}}
	      ]
	    }
	  },
	  "aggs" : {
	    "groupBy" : {
	       "terms" : {"field" : "langage.keyword" }
	    }
	  }
	}
	 */
}
