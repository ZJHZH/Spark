package Test;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * date 2019-10-23 21:30<br/>
 *
 * @author ZJHZH
 */
public class Test {
    public static void main(String[] args) {
        char c = getChar("asdsfgdh", 2, 1);
        System.out.println(c);
    }

    public static char getChar(String str, int m, int n) {
        if (m < 1 || n < 1) {
            return '\000';
        }
        HashMap<Character, Integer> map = new HashMap<>();
        ArrayList<Character> list = new ArrayList<>();
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (map.containsKey(c)) {
                map.put(c, map.get(c) + 1);
            } else {
                map.put(c, 1);
            }
            if (map.get(c) >= n) {
                list.add(c);
            }
            if (map.get(c) > n) {
                list.remove(new Character(c));
            }
        }
        if (list.size() < m) return '\000';
        return list.get(m - 1);
    }
}
