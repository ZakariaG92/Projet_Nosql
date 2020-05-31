package data;

public class Feedback {
	
	public String assin;
	public String personId;
	public double note;
	public String feedback;

	public Feedback() {
		
	}
	
	public Feedback(String assin, String personId, double note, String feedback) {
		this.assin = assin;
		this.personId = personId;
		this.note = note;
		this.feedback = feedback;
	}

	public String toJSON() {
		return "{\"assin\" : \"" + assin + "\", " + "\"personId\" : \"" + personId + "\", " + "\"note\" : " + note
				+ ", \"feedback\":\"" + feedback + "\"}";
	}
}
