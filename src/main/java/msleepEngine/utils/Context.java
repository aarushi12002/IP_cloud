package msleepEngine.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

abstract public class Context<K, V> {
	protected Map<K, V> map = new HashMap<K, V>();

	public void setValue(K key, V value) {
		map.put(key, value);
	}

	public V getValue(K key) {
		if (map.containsKey(key))
			return map.get(key);
		return null;
	}

	public String toString() {
		String str = "";
		for (K key : map.keySet()) {
			str = str + String.format("(%s , %s)", key, map.get(key));
		}
		return str;
	}
	
		
	public Set<K> getKeySet() {

		return map.keySet();

	}

	public int getSize() {

		return map.size();

	}

	public abstract <T> T get(String key);


}
