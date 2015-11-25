package appEngine.model;

import java.util.Date;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.google.appengine.api.datastore.Key;

@PersistenceCapable
public class Sensor {
	
	@PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
	private Key key;
	
	@Persistent
    private String uuid;
	
	@Persistent
    private Float value;
	
	@Persistent
    private Date date;

	public Key getKey() {
		return key;
	}

	public void setKey(Key key) {
		this.key = key;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String name) {
		this.uuid = name;
	}

	public float getValue() {
		return value;
	}

	public void setValue(float val) {
		this.value = val;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public Sensor() {
		super();
	}

	
}