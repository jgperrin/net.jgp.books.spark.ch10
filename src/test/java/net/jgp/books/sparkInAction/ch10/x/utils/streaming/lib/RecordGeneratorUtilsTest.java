/**
 * 
 */
package net.jgp.books.sparkInAction.ch10.x.utils.streaming.lib;

import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

import net.jgp.books.spark.ch10.x.utils.streaming.lib.RecordGeneratorUtils;

/**
 * @author jgp
 *
 */
class RecordGeneratorUtilsTest {

  @Test
  void testGetFirstName() {
    for (int i = 0; i < 10; i++) {
      String firstName = RecordGeneratorUtils.getFirstName();
      if (firstName == null) {
        assertFalse(firstName == null, "First name cannot be null");
      }
      System.out.println("First name: " + firstName);
    }
  }
}
