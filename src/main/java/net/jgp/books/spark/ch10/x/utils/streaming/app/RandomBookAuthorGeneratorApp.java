package net.jgp.books.spark.ch10.x.utils.streaming.app;

import net.jgp.books.spark.ch10.x.utils.streaming.lib.FieldType;
import net.jgp.books.spark.ch10.x.utils.streaming.lib.RecordGeneratorUtils;
import net.jgp.books.spark.ch10.x.utils.streaming.lib.RecordStructure;
import net.jgp.books.spark.ch10.x.utils.streaming.lib.RecordWriterUtils;

/**
 * Generates a series of authors and their books, illustrating joint
 * records.
 * 
 * @author jgp
 *
 */
public class RandomBookAuthorGeneratorApp {

  public static void main(String[] args) {
    RecordStructure rsAuthor = new RecordStructure("author")
        .add("id", FieldType.ID)
        .add("fname", FieldType.FIRST_NAME)
        .add("lname", FieldType.LAST_NAME)
        .add("dob", FieldType.DATE_LIVING_PERSON, "MM/dd/yyyy");

    RecordStructure rsBook = new RecordStructure("book", rsAuthor)
        .add("id", FieldType.ID)
        .add("title", FieldType.TITLE)
        .add("authorId", FieldType.LINKED_ID);

    RandomBookAuthorGeneratorApp app = new RandomBookAuthorGeneratorApp();
    app.start(rsAuthor, RecordGeneratorUtils.getRandomInt(4) + 2);
    app.start(rsBook, RecordGeneratorUtils.getRandomInt(10) + 1);
  }

  private void start(RecordStructure rs, int maxRecord) {
    RecordWriterUtils.write(
        rs.getRecordName() + "_" + System.currentTimeMillis() + ".txt",
        rs.getRecords(maxRecord, true));
  }

}
