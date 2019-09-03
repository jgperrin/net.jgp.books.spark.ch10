package net.jgp.books.spark.ch10.x.utils.streaming.lib;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

/**
 * Utility methods to help generate random records.
 * 
 * @author jgp
 */
public class RecordGeneratorUtils {
  private static RecordGeneratorUtils instance = null;
  private static Calendar cal = Calendar.getInstance();

  private List<String> femaleFirstNames;
  private int femaleFirstNamesCount;
  private List<String> maleFirstNames;
  private int maleFirstNamesCount;
  private List<String> lastNames;
  private int lastNamesCount;

  private RecordGeneratorUtils() throws RecordGeneratorException {
    try {
      femaleFirstNames = Files.readAllLines(new File(
          "data/female_first_names.txt").toPath(),
          Charset.defaultCharset());
    } catch (IOException e) {
      throw new RecordGeneratorException("No female first name available",
          e);
    }
    femaleFirstNamesCount = femaleFirstNames.size();

    try {
      maleFirstNames = Files.readAllLines(new File(
          "data/male_first_names.txt").toPath(),
          Charset.defaultCharset());
    } catch (IOException e) {
      throw new RecordGeneratorException("No male first name available", e);
    }
    maleFirstNamesCount = maleFirstNames.size();

    try {
      lastNames = Files.readAllLines(new File(
          "data/last_names.txt").toPath(),
          Charset.defaultCharset());
    } catch (IOException e) {
      throw new RecordGeneratorException("No last name available", e);
    }
    lastNamesCount = lastNames.size();
  }

  private static RecordGeneratorUtils getInstance() {
    if (RecordGeneratorUtils.instance == null) {
      try {
        RecordGeneratorUtils.instance = new RecordGeneratorUtils();
      } catch (RecordGeneratorException e) {
        System.err.println(
            "Error while instantiating the record generator: " + e
                .getMessage());
      }
    }
    return RecordGeneratorUtils.instance;
  }

  private static String[] articles = { "The", "My", "A", "Your", "Their",
      "Our" };
  private static String[] adjectives = { "", "Great", "Beautiful", "Better",
      "Worse", "Gorgeous", "Terrific", "Fantastic", "Nebulous", "Colorful",
      "Terrible", "Natural", "Wild" };
  private static String[] nouns = { "Life", "Trip", "Experience", "Work",
      "Job", "Beach", "Sky" };
  private static int[] daysInMonth = { 31, 28, 31, 30, 31, 30, 31, 31, 30,
      31, 30, 31 };
  private static String[] lang = { "fr", "en", "es", "de", "it", "pt" };

  public static String getLang() {
    return lang[getRandomInt(lang.length)];
  }

  public static int getRecentYears(int i) {
    return cal.get(Calendar.YEAR) - getRandomInt(i);
  }

  public static int getRating() {
    return getRandomInt(3) + 3;
  }

  public static String getRandomSSN() {
    return "" + getRandomInt(10) + getRandomInt(10) + getRandomInt(10) + "-"
        + getRandomInt(10) + getRandomInt(10)
        + "-" + getRandomInt(10) + getRandomInt(10) + getRandomInt(10)
        + getRandomInt(10);
  }

  public static int getRandomInt(int i) {
    return new Random().nextInt(i);
  }

  public static String getFirstName() {
    return getInstance().getFirstName0();
  }

  private String getFirstName0() {
    switch (getRandomInt(2)) {
      case 0:
        return getFemaleFirstName0();
      case 1:
        return getMaleFirstName0();
    }
    return null;
  }

  private String getMaleFirstName0() {
    String firstName = maleFirstNames.get(
        getRandomInt(maleFirstNamesCount));
    if (firstName.startsWith("#") == true) {
      return getMaleFirstName0();
    }
    return firstName;
  }

  private String getFemaleFirstName0() {
    String firstName = femaleFirstNames.get(
        getRandomInt(femaleFirstNamesCount));
    if (firstName.startsWith("#") == true) {
      return getFemaleFirstName0();
    }
    return firstName;
  }

  public static String getLastName() {
    return getInstance().getLastName0();
  }

  private String getLastName0() {
    String name = lastNames.get(getRandomInt(lastNamesCount));
    if (name.startsWith("#") == true) {
      return getLastName0();
    }
    return name;
  }

  public static String getArticle() {
    return articles[getRandomInt(articles.length)];
  }

  public static String getAdjective() {
    return adjectives[getRandomInt(adjectives.length)];
  }

  public static String getNoun() {
    return nouns[getRandomInt(nouns.length)];
  }

  public static String getTitle() {
    return (getArticle() + " " + getAdjective()).trim() + " " + getNoun();
  }

  public static int getIdentifier(List<Integer> identifiers) {
    int i;
    do {
      i = getRandomInt(RecordGeneratorK.MAX_ID);
    } while (identifiers.contains(i));

    return i;
  }

  public static Integer getLinkedIdentifier(List<
      Integer> linkedIdentifiers) {
    if (linkedIdentifiers == null) {
      return -1;
    }
    if (linkedIdentifiers.isEmpty()) {
      return -2;
    }
    int i = getRandomInt(linkedIdentifiers.size());
    return linkedIdentifiers.get(i);
  }

  /**
   * Generates a date of birth for a living person, based on a format.
   * 
   * @param format
   * @return
   */
  public static String getLivingPersonDateOfBirth(String format) {
    int year = cal.get(Calendar.YEAR) - getRandomInt(
        RecordGeneratorK.MAX_AGE);
    int month = getRandomInt(12);
    int day = getRandomInt(daysInMonth[month]) + 1;
    cal.set(year, month, day);

    SimpleDateFormat sdf = new SimpleDateFormat(format);
    return sdf.format(cal.getTime());
  }

}
