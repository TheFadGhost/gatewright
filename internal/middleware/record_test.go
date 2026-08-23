package middleware

import (
	"context"
	"testing"
)

func TestRecordRoundTrip(t *testing.T) {
	ctx := context.Background()
	if got := RecordFrom(ctx); got != nil {
		t.Fatalf("RecordFrom without record = %+v, want nil", got)
	}
	want := &Record{}
	ctx = WithRecord(ctx, want)
	got := RecordFrom(ctx)
	if got != want {
		t.Fatalf("RecordFrom returned %#v, want the installed pointer", got)
	}
}

func TestRecordCodeOnlyWithRecord(t *testing.T) {
	ctx := WithRecord(context.Background(), &Record{})
	recordCode(ctx, "RATE001")
	if got := RecordFrom(ctx).Fields.Code; got != "RATE001" {
		t.Fatalf("code = %q, want RATE001", got)
	}
	// No record installed: must be a no-op, never a panic.
	recordCode(context.Background(), "UP004")
}

func TestTraceAndRecordCoexist(t *testing.T) {
	tr := &Trace{Enabled: true}
	rec := &Record{}
	ctx := WithRecord(WithTrace(context.Background(), tr), rec)
	if TraceFrom(ctx) != tr || RecordFrom(ctx) != rec {
		t.Fatal("trace and record interfered with each other")
	}
	RecordStage(ctx, OrderNames[PosRequestID], 1234)
	if len(tr.Stages) != 1 || tr.Stages[0].Name != "request-id" {
		t.Fatalf("stage trace missing: %+v", tr.Stages)
	}
}
