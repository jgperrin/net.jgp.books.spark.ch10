package net.jgp.books.sparkInAction.ch10.x.utils.recordGenerator;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

/**
 * Utility methods to help generate random records.
 * 
 * @author jgp
 */
public class RecordGeneratorUtils {

	private static RecordGeneratorUtils instance = null;

	private List<String> femaleFirstNames;
	private int femaleFirstNamesCount;
	private List<String> maleFirstNames;
	private int maleFirstNamesCount;

	private RecordGeneratorUtils() throws RecordGeneratorException {
		try {
			femaleFirstNames = Files.readAllLines(new File("data/female_first_names.txt").toPath(),
					Charset.defaultCharset());
		} catch (IOException e) {
			throw new RecordGeneratorException("No female first name available", e);
		}
		femaleFirstNamesCount = femaleFirstNames.size();

		try {
			maleFirstNames = Files.readAllLines(new File("data/male_first_names.txt").toPath(),
					Charset.defaultCharset());
		} catch (IOException e) {
			throw new RecordGeneratorException("No male first name available", e);
		}
		maleFirstNamesCount = maleFirstNames.size();
	}

	private static RecordGeneratorUtils getInstance() {
		if (RecordGeneratorUtils.instance == null) {
			try {
				RecordGeneratorUtils.instance = new RecordGeneratorUtils();
			} catch (RecordGeneratorException e) {
			      System.err.println("Error while instantiating the record generator: " + e.getMessage());
			}
		}
		return RecordGeneratorUtils.instance;
	}

	private static String[] lnames = { "Smith", "Mills", "Perrin", "Foster", "Kumar", "Jones", "Tutt", "Main", "Haque",
			"Christie", "Khan", "Kahn", "Hahn", "Sanders" };
	private static String[] articles = { "The", "My", "A", "Your", "Their" };
	private static String[] adjectives = { "", "Great", "Beautiful", "Better", "Worse", "Gorgeous", "Terrific",
			"Terrible", "Natural", "Wild" };
	private static String[] nouns = { "Life", "Trip", "Experience", "Work", "Job", "Beach" };
	private static int[] daysInMonth = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

	public static String getRandomSSN() {
		return "" + getRandomInt(10) + getRandomInt(10) + getRandomInt(10) + "-" + getRandomInt(10) + getRandomInt(10)
				+ "-" + getRandomInt(10) + getRandomInt(10) + getRandomInt(10) + getRandomInt(10);
	}

	public static int getRandomInt(int i) {
		return (int) (Math.random() * i);
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
		String firstName = maleFirstNames.get(getRandomInt(maleFirstNamesCount));
		if (firstName.startsWith("#") == true) {
			return getMaleFirstName0();
		}
		return firstName;
	}

	private String getFemaleFirstName0() {
		String firstName = femaleFirstNames.get(getRandomInt(femaleFirstNamesCount));
		if (firstName.startsWith("#") == true) {
			return getFemaleFirstName0();
		}
		return firstName;
	}

	public static String getLastName() {
		return lnames[getRandomInt(lnames.length)];
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
			i = getRandomInt(60000);
		} while (identifiers.contains(i));

		return i;
	}

	public static Integer getLinkedIdentifier(List<Integer> linkedIdentifiers) {
		if (linkedIdentifiers == null) {
			return -1;
		}
		if (linkedIdentifiers.isEmpty()) {
			return -2;
		}
		int i = getRandomInt(linkedIdentifiers.size());
		return linkedIdentifiers.get(i);
	}

	public static String getLivingPersonDateOfBirth(String format) {
		Calendar d = Calendar.getInstance();
		int year = d.get(Calendar.YEAR) - getRandomInt(120) + 15;
		int month = getRandomInt(12);
		int day = getRandomInt(daysInMonth[month]) + 1;
		d.set(year, month, day);

		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(d.getTime());
	}

}
