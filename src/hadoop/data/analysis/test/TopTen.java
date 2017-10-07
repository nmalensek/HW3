package hadoop.data.analysis.test;

import org.apache.hadoop.util.hash.Hash;

import java.util.ArrayList;
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
        HashMap<String, Integer> topTenMap = new HashMap<>();
        for (String g : genres) {
            String tag = g.split(":")[0];
            int count = Integer.parseInt(g.split(":")[1]);
            topTenMap.put(tag, count);
        }

        ArrayList<String> tenList = new ArrayList<>();

        HashMap<String, Integer> tenCopy = new HashMap<>(topTenMap);
        for (int i = 0; i < 10; i++) {
            int largest = 0;
            String largestGenre = "";
            for (String genre : tenCopy.keySet()) {
                if (tenCopy.get(genre) > largest) {
                    largest = tenCopy.get(genre);
                    largestGenre = genre;
                }
            }
            tenList.add(largestGenre + ":" + largest);
            tenCopy.remove(largestGenre);
        }

        System.out.println(tenList.toString());
    }

    public static void main(String[] args) {
        TopTen topTen = new TopTen();
        topTen.calcTopTen();
    }
}
