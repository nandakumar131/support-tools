package org.preta.tools.ozone.db.debugger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class FileParser {

  Map<Long, String> containerStateMap;
  Map<Long, Long> deletedBlocksMap;

  public FileParser() {
    containerStateMap = new HashMap<>();
    deletedBlocksMap = new HashMap<>();
  }


  public void parseContainerStateFile(String fileName) throws Exception {
      FileInputStream fis = new FileInputStream(fileName);
      BufferedReader br = new BufferedReader(new InputStreamReader(fis));
      String line = null;
      while ((line = br.readLine()) != null) {
        String[] containerState = line.split(",");
        containerStateMap.put(Long.parseLong(containerState[0]), containerState[1].trim());
      }
      br.close();
  }

  public void parseDeletedBlockFile(String fileName) throws Exception {
        FileInputStream fis = new FileInputStream(fileName);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        String line = null;
        while ((line = br.readLine()) != null) {
            String[] containerState = line.split(",");
          deletedBlocksMap.put(Long.parseLong(containerState[0]), Long.parseLong(containerState[1]));
        }
        br.close();
  }

  public void printContainerState() {
    System.out.println("Container State Map");
    for (Map.Entry<Long, String> entry : containerStateMap.entrySet()) {
      System.out.println("Container ID: " + entry.getKey() + " State: " + entry.getValue());
      break;
    }
    System.out.println(containerStateMap.size());
  }

  public void printDeletedBlocks() throws IOException {
    System.out.println("Deleted Blocks Map");
    try (BufferedWriter writer = new BufferedWriter(
        new FileWriter("/Users/nvadivelu/Issues/BYD-ENGESC-25426/20240924/deleted-blocks-with-container-state-deleted.csv"))) {
      for (Map.Entry<Long, Long> entry : deletedBlocksMap.entrySet()) {
        if (containerStateMap.get(entry.getKey()).equals("DELETED")) {
          writer.write("Container ID: " + entry.getKey() + " Block Count: " +
                entry.getValue() + " Container State: " +
                containerStateMap.get(entry.getKey()));
          writer.newLine();
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    FileParser parser = new FileParser();
    parser.parseContainerStateFile("/Users/nvadivelu/Issues/BYD-ENGESC-25426/20240924/container-state.csv");
    parser.parseDeletedBlockFile("/Users/nvadivelu/Issues/BYD-ENGESC-25426/20240924/deleted-blocks.csv");
    parser.printContainerState();
    parser.printDeletedBlocks();
  }
}
