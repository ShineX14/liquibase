package com.ebao.tool.liquibase.util;


public class PropertyParser {
  public PropertyPath parseProperty(String skey) {
    PropertyPath path = new PropertyPath();

    String table = null;
    String subdir = null;
    String filename = null;

    int index1 = skey.indexOf('[');
    if (index1 > 0) {
      int index2 = skey.indexOf(']');
      if (index2 < 0) {
        throw new IllegalArgumentException(skey);
      }

      table = skey.substring(0, index1).toUpperCase();

      String part2 = skey.substring(index1 + 1, index2);
      int index3 = part2.lastIndexOf('/');
      if (index3 < 0) {//e.g., index
        filename = part2;
      } else if (index3 == 0) {//e.g., /index
        filename = part2.substring(1);
      } else if (index3 == part2.length() - 1) {//e.g., index/
        subdir = part2.substring(0, part2.length() - 1);
        filename = table;
      } else if (index3 > 0) {//e.g., index/index
        subdir = part2.substring(0, index3);
        filename = part2.substring(index3 + 1);
      }

      if (filename.startsWith(".")) {
        filename = table + filename;
      }
    } else {
      table = skey.toUpperCase();
      filename = table;
    }

    path.table = table;
    path.dir = subdir;
    path.filename = filename;
    return path;
  }

}
