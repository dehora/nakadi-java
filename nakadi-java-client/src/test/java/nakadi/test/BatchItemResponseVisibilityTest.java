package nakadi.test;

import java.util.EnumSet;
import nakadi.BatchItemResponse;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BatchItemResponseVisibilityTest {


  @Test
  public void visibility_gh264() {
    // https://github.com/dehora/nakadi-java/issues/264
    EnumSet<BatchItemResponse.PublishingStatus> errors = EnumSet.of(
        BatchItemResponse.PublishingStatus.failed,
        BatchItemResponse.PublishingStatus.aborted
    );

    assertTrue(errors.size() == 2);
  }

}
