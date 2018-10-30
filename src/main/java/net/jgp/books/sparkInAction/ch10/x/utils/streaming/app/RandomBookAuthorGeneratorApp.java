package net.jgp.books.sparkInAction.ch10.x.utils.streaming.app;

import net.jgp.books.sparkInAction.ch10.x.utils.streaming.lib.RecordGeneratorUtils;
import net.jgp.books.sparkInAction.ch10.x.utils.streaming.lib.RecordStructure;
import net.jgp.books.sparkInAction.ch10.x.utils.streaming.lib.RecordType;
import net.jgp.books.sparkInAction.ch10.x.utils.streaming.lib.RecordWriterUtils;

public class RandomBookAuthorGeneratorApp {

  public static void main(String[] args) {

    RecordStructure rsAuthor = new RecordStructure("author");
    rsAuthor.add("id", RecordType.ID);
    rsAuthor.add("fname", RecordType.FIRST_NAME);
    rsAuthor.add("lname", RecordType.LAST_NAME);
    rsAuthor.add("dob", RecordType.DATE_LIVING_PERSON, "MM/dd/yyyy");

    RecordStructure rsBook = new RecordStructure("book", rsAuthor);
    rsBook.add("id", RecordType.ID);
    rsBook.add("title", RecordType.TITLE);
    rsBook.add("authorId", RecordType.LINKED_ID);

    RandomBookAuthorGeneratorApp app = new RandomBookAuthorGeneratorApp();
    app.start(rsAuthor);
    app.start(rsBook);
  }

  private void start(RecordStructure rs) {
    int maxRecord = RecordGeneratorUtils.getRandomInt(100) + 1;
    RecordWriterUtils.write(
        rs.getRecordName() + "_" + System.currentTimeMillis() + ".txt",
        rs.getRecords(maxRecord, true));
  }

}
