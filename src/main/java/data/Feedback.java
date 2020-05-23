package data;

public class Feedback {
	public String assin;
	public String personId;
	public String feedback;

	public Feedback(String assin, String personId, String feedback) {
		this.assin = assin;
		this.personId = personId;
		this.feedback = feedback;
	}

	public String toJSON() {
		return "{\"assin\" : \"" + assin + "\", " + "\"personId\" : \"" + personId + "\", " + "\"feedback\":\""
				+ feedback + "\"}";
	}
}
