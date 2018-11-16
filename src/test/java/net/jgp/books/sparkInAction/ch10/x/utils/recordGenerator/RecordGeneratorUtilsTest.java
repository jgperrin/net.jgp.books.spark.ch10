/**
 * 
 */
package net.jgp.books.sparkInAction.ch10.x.utils.recordGenerator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.jupiter.api.Test;

import net.jgp.books.sparkInAction.ch10.x.utils.streaming.lib.RecordGeneratorUtils;

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
        assertFalse("First name cannot be null", firstName == null);
      }
      System.out.println("First name: " + firstName);
    }
    assertTrue("Displayed first names", true);
  }

}
