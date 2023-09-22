package com.ksc.wordcount.util;

import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {
    public final static String regex = "http://[\\w\\d\\-]+(\\.[\\w\\d\\-]+)+([\\w\\d\\-.,@?^=%&:/~+#]*[\\w\\d\\-@?^=%&/~+#])?";

    public static String[] getUrlFromLine(String input) {
        List<String> ls = new LinkedList<>();
        Matcher matcher = Pattern.compile(regex).matcher(input);
        while (matcher.find()) {
            String url = matcher.group();
            ls.add(url);
        }
        return ls.toArray(new String[0]);
    }

    public static long getMemory(String value) {
        if (value == null || value.length() == 0) return 0;

        value = value.toLowerCase(Locale.ROOT);
        int index = lastNumIndex(value);
        if (index == -1) {
            return 0;
        }
        long num = Long.parseLong(value.substring(0, index + 1));
        String suffix = value.substring(index + 1);
        if ("m".equals(suffix)) {
            num = num * 1024 * 1024;
        }
        return num;
    }

    public static int lastNumIndex(String input) {
        for (int i = input.length() - 1; i >= 0; i--) {
            char c = input.charAt(i);
            if (c >= '0' && c <= '9') {
                return i;
            }
        }
        return -1;
    }
}
