package org.linkja.hashing;

import org.junit.jupiter.api.Test;
import org.linkja.hashing.steps.IStep;
import org.mockito.Mockito;

import java.util.ArrayList;

import static org.mockito.Mockito.*;

class RecordProcessorTest {

  class FakeStep implements IStep {
    public DataRow run(DataRow row) { return row; }

    @Override
    public String getStepName() {
      return this.getClass().getSimpleName();
    }
  }

  @Test
  void run() {
    FakeStep fakeStep1 = mock(FakeStep.class);
    when(fakeStep1.run(Mockito.any(DataRow.class))).thenAnswer(invoke -> new DataRow());
    FakeStep fakeStep2 = mock(FakeStep.class);
    when(fakeStep1.run(Mockito.any(DataRow.class))).thenAnswer(invoke -> new DataRow());
    ArrayList<IStep> steps = new ArrayList<IStep>() {{
      add(fakeStep1);
      add(fakeStep2);
    }};
    RecordProcessor processor = new RecordProcessor(steps);
    DataRow row = new DataRow();
    processor.run(row);

    // Ensure that each of our steps was called once
    verify(fakeStep1, times(1)).run(row);
    verify(fakeStep2, times(1)).run(row);
  }

  @Test
  void run_NullSteps() {
    ArrayList<IStep> steps = new ArrayList<IStep>() {{
      add(null);
      add(null);
    }};
    RecordProcessor processor = new RecordProcessor(steps);
    DataRow row = new DataRow();
    processor.run(row);  // Passes if no exception thrown (means we guarded against null objects)
  }
}