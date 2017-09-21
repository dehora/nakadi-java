package nakadi;

import org.junit.Test;

import static org.junit.Assert.*;

public class BatchItemResponseTest {


  @Test
  public void statusStep() {

    BatchItemResponse bir = new BatchItemResponse();
    bir.publishingStatus(BatchItemResponse.PublishingStatus.failed);
    bir.step(BatchItemResponse.Step.partitioning);

    assertEquals(bir.publishingStatus(), BatchItemResponse.PublishingStatus.failed);
    assertEquals(bir.step(), BatchItemResponse.Step.partitioning);
  }

}
