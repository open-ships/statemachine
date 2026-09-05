package supervised

import (
	"context"
	"testing"
)

func TestStatusExposesTripReportingLifetime(t *testing.T) {
	var supervisor *Supervisor[testState, testEvent, *testData]
	observed := false
	options := Options[testState, testEvent]{Limits: limits(), Unjournaled: true,
		Recorder: func(_ context.Context, record Record[testState, testEvent]) error {
			if record.Kind == RecordTrip {
				status := supervisor.Status()
				observed = status.ReportingRunning && status.RecorderRunning
			}
			return nil
		},
	}
	var err error
	supervisor, err = NewWithOptions(MustCompile(validDefinition()), options)
	if err != nil {
		t.Fatal(err)
	}
	supervisor.Trip(errTrip)
	if !observed {
		t.Fatal("Trip recorder did not expose reporting ownership")
	}
	if status := supervisor.Status(); status.ReportingRunning || status.RecorderRunning {
		t.Fatalf("finished Trip retained reporting health: %+v", status)
	}
}
