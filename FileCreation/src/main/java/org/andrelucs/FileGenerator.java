package org.andrelucs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FileGenerator {

    public static File[] generateFiles(int split, int n, String alphabet, int minSize, int maxSize) {
        List<String> words = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            words.add(generateRandomWord(alphabet, minSize, maxSize));
        }

        int count = 0;
        File[] files = new File[split];
        for (int i = 0; i < split; i++) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter("output" + i + ".txt"))) {
                files[i] = new File("output" + i + ".txt");
                for (int j = 0; j < n / split; j++) {
                    writer.write(words.get(count) + " ");
                    count++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return files;
    }

    private static String generateRandomWord(String possibleLetters, int min, int max) {
        Random rand = new Random();
        int wordLength = rand.nextInt(max - min) + min;
        StringBuilder word = new StringBuilder();
        for (int i = 0; i < wordLength; i++) {
            word.append(possibleLetters.charAt(rand.nextInt(possibleLetters.length())));
        }
        return word.toString();
    }

    public static void main(String[] args) {
        generateFiles(5, 100, "abcdefghijklmnopqrstuvwxyz", 3, 8);
    }
}