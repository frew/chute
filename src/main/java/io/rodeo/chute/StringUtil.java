package io.rodeo.chute;

public class StringUtil {
	public static String mkString(Object[] arr, String delim) {
		if (arr.length == 0) {
			return "";
		}

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < arr.length - 1; i++) {
			sb.append(arr[i]);
			sb.append(delim);
		}
		sb.append(arr[arr.length - 1]);
		return sb.toString();
	}
}
