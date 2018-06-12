package util;


public class JoinFileUtil {
	
	private String source;
	private String separator;
	private String key;
	
	
	private String[] selectedAttributes;
	private String[] headers;
	
	
	public JoinFileUtil(String source,String separator) {
		this.source = source;
		this.separator = separator;
		this.headers = null;
		this.selectedAttributes = null;
		
	}
	
	public void setHeaders(String str) {
		headers = str.split(separator);
	}
	
	public void setSelectedAttributes(String str) {
		selectedAttributes = str.split(separator);
	}
	
	public int getIndexOf(String attribute) {
		for(int i =0; i < headers.length;i++)
			if(attribute.equals(headers[i]))
					return i;
		
		return -1;
	}
	
	public int getIndexOfAfterSelection(String attribute) {
		for(int i =0; i < getSelectedAttributes().length;i++)
			if(attribute.equals(getSelectedAttributes()[i]))
					return i;
		
		return -1;
	}
	
	
	public String[] getHeaders() {
		return headers;
	}
	
	public  String[] getSelectedAttributes() {
		if(selectedAttributes == null)return headers;
		return selectedAttributes;
	}
	
	public String getSeparator() {
		return separator;
	}
	
	public String getSource() {
		return source;
	}
	
	public void setKey(String key) {
		this.key = key;
	}
	
	public String getKey() {
		return key;
	}

	public int getIndexOfKey() {
		return getIndexOf(key);
	}

	public int getIndexOfKeyAfterSelection() {
		return getIndexOfAfterSelection(key);
	}
}
