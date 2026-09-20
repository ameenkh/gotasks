package dashboard

// Handler tests against real MongoDB (skip when unreachable), driving the
// JSON API end to end over seeded state.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ameenkh/gotasks"
	"github.com/ameenkh/gotasks/stores/mongostore"
)

func testSetup(t *testing.T) (*mongostore.Store, *gotasks.Manager, *httptest.Server) {
	t.Helper()
	uri := os.Getenv("GOTASKS_TEST_MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ns := fmt.Sprintf("ui_%d", time.Now().UnixNano())
	st, err := mongostore.New(ctx, uri,
		mongostore.WithDatabase("gotasks_test"),
		mongostore.WithNamespace(ns),
	)
	if err != nil {
		t.Skipf("no MongoDB at %s: %v", uri, err)
	}
	m, err := gotasks.New(st,
		gotasks.WithQueues(
			gotasks.QueuePolicy{Name: "emails", TTL: time.Hour, MaxAttempts: 2},
			gotasks.QueuePolicy{Name: "reports"},
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv := httptest.NewServer(Handler(st))
	t.Cleanup(func() {
		srv.Close()
		bg := context.Background()
		_ = st.DropCollection(bg)
		_ = st.Close(bg)
	})
	return st, m, srv
}

func call(t *testing.T, method, url string, wantStatus int, out any) {
	t.Helper()
	req, _ := http.NewRequest(method, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s: status %d, want %d", method, url, resp.StatusCode, wantStatus)
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("decode: %v", err)
		}
	}
}

func TestDashboardAPI(t *testing.T) {
	st, m, srv := testSetup(t)
	ctx := context.Background()

	// Seed: 3 pending emails, 1 dead email, 1 pending report.
	ids, err := gotasks.EnqueueMany(ctx, m, "emails", "send", make([]struct{}, 3))
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	deadID, _ := gotasks.Enqueue(ctx, m, "emails", "send", struct{}{}, gotasks.TaskPolicy{MaxAttempts: 1})
	if _, err := gotasks.Enqueue(ctx, m, "reports", "generate", struct{}{}); err != nil {
		t.Fatalf("seed report: %v", err)
	}
	// Kill one task to dead via the store path.
	task, err := st.Claim(ctx, gotasks.ClaimOptions{WorkerID: "w", Queues: []string{"emails"}, Lease: time.Minute})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	te := gotasks.TaskError{At: time.Now(), Attempt: 1, Worker: "w", Message: "boom"}
	if err := st.Fail(ctx, task, te, time.Now(), true); err != nil {
		t.Fatalf("fail: %v", err)
	}
	_ = deadID

	// Overview: registered queues with counts.
	var ov struct {
		Queues []mongostore.QueueOverview `json:"queues"`
		Next   string                     `json:"next"`
		Total  int64                      `json:"total"`
	}
	call(t, "GET", srv.URL+"/api/overview", 200, &ov)
	if len(ov.Queues) != 2 {
		t.Fatalf("overview queues = %d, want 2", len(ov.Queues))
	}
	byName := map[string]mongostore.QueueOverview{}
	for _, q := range ov.Queues {
		byName[q.Name] = q
	}
	if e := byName["emails"]; e.Pending != 3 || e.Dead != 1 || e.MaxAttempts != 2 || e.TTL != time.Hour {
		t.Errorf("emails overview wrong: %+v", e)
	}
	if r := byName["reports"]; r.Pending != 1 {
		t.Errorf("reports overview wrong: %+v", r)
	}
	// Overview pagination: page size 1 walks both queues.
	call(t, "GET", srv.URL+"/api/overview?limit=1", 200, &ov)
	if len(ov.Queues) != 1 || ov.Next != ov.Queues[0].Name || ov.Total != 2 {
		t.Fatalf("page 1 wrong: %+v next=%q total=%d", ov.Queues, ov.Next, ov.Total)
	}
	call(t, "GET", srv.URL+"/api/overview?limit=1&after="+ov.Next, 200, &ov)
	if len(ov.Queues) != 1 {
		t.Fatalf("page 2 wrong: %+v", ov.Queues)
	}
	// Queue detail page data.
	var det mongostore.QueueOverview
	call(t, "GET", srv.URL+"/api/queues/emails", 200, &det)
	if det.Name != "emails" || det.Pending != 3 || det.TTL != time.Hour {
		t.Errorf("queue detail wrong: %+v", det)
	}
	call(t, "GET", srv.URL+"/api/queues/nope", 404, nil)

	// Task list with filters + pagination shape.
	var list struct {
		Tasks []gotasks.Task `json:"tasks"`
		Next  string         `json:"next"`
		Total int64          `json:"total"`
	}
	call(t, "GET", srv.URL+"/api/tasks?queue=emails&status=pending", 200, &list)
	if len(list.Tasks) != 3 || list.Next == "" || list.Total != 3 {
		t.Fatalf("filtered list = %d tasks (next %q, total %d), want 3/3", len(list.Tasks), list.Next, list.Total)
	}
	// Total ignores the pagination cursor.
	call(t, "GET", srv.URL+"/api/tasks?queue=emails&status=pending&limit=2", 200, &list)
	if len(list.Tasks) != 2 || list.Total != 3 {
		t.Fatalf("paged list = %d (total %d), want 2 (total 3)", len(list.Tasks), list.Total)
	}

	// Detail.
	var one gotasks.Task
	call(t, "GET", srv.URL+"/api/tasks/"+ids[0], 200, &one)
	if one.ID != ids[0] || one.Queue != "emails" {
		t.Fatalf("get task: %+v", one)
	}
	call(t, "GET", srv.URL+"/api/tasks/aaaaaaaaaaaaaaaaaaaaaaaa", 404, nil)

	// Requeue the dead task via the API, then verify it is pending again.
	call(t, "GET", srv.URL+"/api/tasks?queue=emails&status=dead", 200, &list)
	if len(list.Tasks) != 1 {
		t.Fatalf("dead list = %d, want 1", len(list.Tasks))
	}
	call(t, "POST", srv.URL+"/api/tasks/"+list.Tasks[0].ID+"/requeue", 200, nil)
	call(t, "GET", srv.URL+"/api/tasks?queue=emails&status=dead", 200, &list)
	if len(list.Tasks) != 0 {
		t.Fatalf("dead list after requeue = %d, want 0", len(list.Tasks))
	}

	// Delete one pending task.
	call(t, "DELETE", srv.URL+"/api/tasks/"+ids[1], 200, nil)
	call(t, "GET", srv.URL+"/api/tasks/"+ids[1], 404, nil)

	// Metrics endpoint shape (empty is fine — no janitor ran here).
	var mm struct {
		Points []mongostore.MetricPoint `json:"points"`
	}
	call(t, "GET", srv.URL+"/api/metrics?queue=emails&since=1h", 200, &mm)
	call(t, "GET", srv.URL+"/api/metrics", 400, nil)

	// Index page served.
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("index: %v", err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 2048)
	n, _ := resp.Body.Read(buf)
	if resp.StatusCode != 200 || !strings.Contains(string(buf[:n]), "gotasks") {
		t.Fatalf("index page wrong: %d", resp.StatusCode)
	}
}

func TestRequeueDeadByQueueAPI(t *testing.T) {
	st, m, srv := testSetup(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := gotasks.Enqueue(ctx, m, "emails", "send", struct{}{}, gotasks.TaskPolicy{MaxAttempts: 1}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		task, err := st.Claim(ctx, gotasks.ClaimOptions{WorkerID: "w", Lease: time.Minute})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		te := gotasks.TaskError{At: time.Now(), Attempt: 1, Worker: "w", Message: "boom"}
		if err := st.Fail(ctx, task, te, time.Now(), true); err != nil {
			t.Fatalf("fail: %v", err)
		}
	}
	var out struct {
		Requeued int64 `json:"requeued"`
	}
	call(t, "POST", srv.URL+"/api/queues/emails/requeue-dead", 200, &out)
	if out.Requeued != 3 {
		t.Fatalf("requeued = %d, want 3", out.Requeued)
	}
}
