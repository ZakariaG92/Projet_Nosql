package data;

public class Post {
	public String id;
	public String imageFile;
	public String creationDate;
	public String location;
	public String browserUsed;
	public String langage;
	public String content;
	public Integer length;

	public Post(String id, String imageFile, String creationDate, String location, String browserUsed, String langage,
			String content, Integer length) {
		super();
		this.id = id;
		this.imageFile = imageFile;
		this.creationDate = creationDate;
		this.location = location;
		this.browserUsed = browserUsed;
		this.langage = langage;
		this.content = content;
		this.length = length;
	}

}
