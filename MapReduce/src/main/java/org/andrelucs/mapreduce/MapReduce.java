package org.andrelucs.mapreduce;

import org.andrelucs.FileGenerator;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

public class MapReduce {
    public void MapReduce(File[] files, BiFunction<File, BufferedWriter, Void> mapFunction, BiFunction<String, String[], Void> reduceFunction) throws IOException {
        MapReduce(files, mapFunction, reduceFunction, false);
    }

    public void MapReduce(File[] files, BiFunction<File, BufferedWriter, Void> mapFunction, BiFunction<String, String[], Void> reduceFunction, boolean deleteInputFiles) throws IOException {
        File tempFile =  new File("./temp.txt");

        try {
            tempFile.createNewFile();
            tempFile.deleteOnExit();

            ExecutorService execService = Executors.newVirtualThreadPerTaskExecutor();

            var threads = new ArrayList<Thread>();

            BufferedWriter tempBw = new BufferedWriter(new FileWriter(tempFile));

            for (File file : files) {
                execService.execute(()-> mapFunction.apply(file, tempBw));
                if(deleteInputFiles) file.deleteOnExit();
            }

            execService.shutdown();
            execService.awaitTermination(1, TimeUnit.MINUTES);
            tempBw.close();

            var map = new HashMap<String, List<String>>();

            BufferedReader reader = new BufferedReader(new FileReader(tempFile));

            while(true) {
                String line = reader.readLine();
                if (line == null) break;

                String[] parts = line.split(":");
                if (map.containsKey(parts[0])) {
                    map.get(parts[0]).add(parts[1]);
                }else{
                    List<String> list = new ArrayList<>();
                    list.add(parts[1]);
                    map.put(parts[0], list);
                }
            }

            execService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

            for(String key : map.keySet()){
                execService.execute(()->reduceFunction.apply(key, map.get(key).toArray(new String[0])));
            }

            execService.shutdown();
            execService.awaitTermination(1, TimeUnit.MINUTES);

            reader.close();
            System.out.println("Result available in : " + tempFile.getAbsolutePath());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


    }

    public static void main(String[] args) throws IOException {

        var r = new File("result.txt");
        if (r.exists()) r.delete();


        BufferedWriter bw = new BufferedWriter(new FileWriter("result.txt"));
        bw.write("Results:\n");
        bw.flush();

        File[] files = FileGenerator.generateFiles(8, 100, "abcd", 3, 5);


        BiFunction<File, BufferedWriter, Void> mapFunction = createMapFunction();
        BiFunction<String, String[], Void> reduceFunction = createReduceFunction(bw);

        var countingWords = new MapReduce();
        countingWords.MapReduce(files, mapFunction, reduceFunction, true);
    }

    private static BiFunction<File, BufferedWriter, Void> createMapFunction() {
        return (File file, BufferedWriter tempbw) -> {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(file));
                while (true) {
                    String line = reader.readLine();
                    if (line == null) {
                        break;
                    }

                    String[] words = line.split(" ");

                    for (String word : words) {
                        word = word.trim();
                        if (word.isEmpty()) {
                            continue;
                        }
                        tempbw.write(word + ":1\n");
                        tempbw.flush();
                    }

                }
                reader.close();
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return null;
        };
    }

    private static BiFunction<String, String[], Void> createReduceFunction(BufferedWriter bw) {
        return (String key, String[] values) -> {
            try {
                bw.write(key + " : " + values.length+'\n');
                bw.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        };
    }
}