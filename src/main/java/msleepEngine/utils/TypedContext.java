package msleepEngine.utils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONObject;


public class TypedContext extends Context <String, Object> {
	
	private Map<String, String> typeMap = new LinkedHashMap<String, String>(); 

	public Set<String> getSchema(){
		return typeMap.keySet();
	}
	
	public String getType(String key){
		return typeMap.get(key);
	}
	
	
	public boolean containsKey(String key){
		return getMap().containsKey(key);
	}

	public String getDBType(String key){
		String type = this.getType(key);
		switch(type){
		case "String":
			return "text";
		case "Long":
			return "bigint";
		case "Date":
			return "timestamp";
		case "Map":
			return "map<text, text>";
		case "TimeMap":
			return "map<text, timestamp>";
		case "List":
			return "list<text>";
		case "Int":
			return "int";
		case "Float":
			return "float";
		default:
			return type;
		}
	}

	
	public void setType(String key, String type){
		typeMap.put(key, type);
	}
	
	public void setValue(String key, Object value, String type) {
		typeMap.put(key, type);
		super.setValue(key, value);
	}

	
	public void setValue(String key, Object value) {
		this.setValue(key, value, "String");
	}

	public Object getValue(String key) {
		return super.getValue(key);
	}

	//String, Long, Date, Map, List, Set, Boolean
	
	@SuppressWarnings("unchecked")
	public <T> T get(String key) {
		return (T)super.getValue(key);
	}
	
	public static String normalizeKey(String key){
		return key.replaceAll("[/:]", "_");
	}
	
	
	public Map<String, Object> getMap(){
		return super.map;
	}

	@SuppressWarnings("unchecked")
	public <T> T readAndRemove(String key){
		T value = (T)super.getValue(key);
		super.map.remove(key);
		return value;
	}


	public void remove(String key){
		super.map.remove(key);
	}
	
	public String getSchemaString(){
		String txt = new String();
		for (String key : this.getSchema()){
			txt += String.format("%s %s,", key, this.getDBType(key));
		}
		return txt;
	}
	
	
	@SuppressWarnings("unchecked")
	public JSONObject toJSON(){
		JSONObject json = new JSONObject();
		json.putAll(getMap());
		return json;
	}
}