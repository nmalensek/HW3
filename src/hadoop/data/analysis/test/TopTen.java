package hadoop.data.analysis.test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;

public class TopTen {


    String[] genres = {
            "Folk:5",
            "Rock:1",
            "Rap:3",
            "Techno:5",
            "Classical:2",
            "Metal:10",
            "Dance:4",
            "Pop:9",
            "Singer:5",
            "Vocal:8",
            "Country:11",
            "Cover:2",
            "Dubstep:13",
    };

    private void calcTopTen() {
        HashMap<Integer, String> topTenMap = new HashMap<>();
        for (String g : genres) {
            String tag = g.split(":")[0];
            int count = Integer.parseInt(g.split(":")[1]);
            topTenMap.put(count, tag);
        }

        LinkedList<Integer> tenList = new LinkedList<>();

        tenList.addAll(topTenMap.keySet());
        Collections.sort(tenList);
        Collections.reverse(tenList);
        for (int i = 0; i < 10; i++) {
            System.out.println(i + 1 + " - " + topTenMap.get(tenList.get(i)) + ":" + tenList.get(i));
        }
    }

    public static void main(String[] args) {
        TopTen topTen = new TopTen();
        topTen.calcTopTen();
    }
}
