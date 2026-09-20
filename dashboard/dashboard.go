// Package dashboard serves the gotasks dashboard: an embeddable http.Handler
// exposing a JSON API and a single-page frontend over the tasks, queues and
// metrics collections. Everything is read from MongoDB — no other backend.
//
// Mount it inside your own service, BEHIND YOUR OWN AUTH (the dashboard can
// requeue and delete tasks; access control is deliberately the host app's
// responsibility):
//
//	mux.Handle("/gotasks/", http.StripPrefix("/gotasks", dashboard.Handler(store)))
package dashboard

import (
	_ "embed"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/ameenkh/gotasks"
	"github.com/ameenkh/gotasks/stores/mongostore"
)

//go:embed index.html
var indexHTML []byte

// Handler returns the dashboard handler for a mongostore-backed deployment.
func Handler(st *mongostore.Store) http.Handler {
	mux := http.NewServeMux()
	h := &api{st: st}

	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})
	mux.HandleFunc("GET /api/overview", h.overview)
	mux.HandleFunc("GET /api/queues/{name}", h.queueDetail)
	mux.HandleFunc("GET /api/tasks", h.listTasks)
	mux.HandleFunc("GET /api/tasks/{id}", h.getTask)
	mux.HandleFunc("POST /api/tasks/{id}/requeue", h.requeueTask)
	mux.HandleFunc("DELETE /api/tasks/{id}", h.deleteTask)
	mux.HandleFunc("POST /api/queues/{name}/requeue-dead", h.requeueDead)
	mux.HandleFunc("GET /api/metrics", h.metrics)
	return mux
}

type api struct {
	st *mongostore.Store
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, gotasks.ErrNotFound) {
		status = http.StatusNotFound
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func (h *api) overview(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	rows, next, total, err := h.st.Overview(r.Context(), r.URL.Query().Get("after"), limit)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queues": rows, "next": next, "total": total})
}

func (h *api) queueDetail(w http.ResponseWriter, r *http.Request) {
	row, err := h.st.QueueDetail(r.Context(), r.PathValue("name"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, row)
}

func (h *api) listTasks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	tasks, total, err := h.st.ListTasks(r.Context(), mongostore.TaskQuery{
		Queue:  q.Get("queue"),
		Status: q.Get("status"),
		Type:   q.Get("type"),
		Limit:  limit,
		Before: q.Get("before"),
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	next := ""
	if len(tasks) > 0 {
		next = tasks[len(tasks)-1].ID
	}
	writeJSON(w, http.StatusOK, map[string]any{"tasks": tasks, "next": next, "total": total})
}

func (h *api) getTask(w http.ResponseWriter, r *http.Request) {
	t, err := h.st.GetTask(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (h *api) requeueTask(w http.ResponseWriter, r *http.Request) {
	if err := h.st.Requeue(r.Context(), r.PathValue("id")); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (h *api) deleteTask(w http.ResponseWriter, r *http.Request) {
	if err := h.st.DeleteTask(r.Context(), r.PathValue("id")); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (h *api) requeueDead(w http.ResponseWriter, r *http.Request) {
	n, err := h.st.RequeueDeadByQueue(r.Context(), r.PathValue("name"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"requeued": n})
}

func (h *api) metrics(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Query().Get("queue")
	if queue == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "queue parameter required"})
		return
	}
	since := time.Hour
	if s := r.URL.Query().Get("since"); s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 && d <= 30*24*time.Hour {
			since = d
		}
	}
	points, err := h.st.MetricsSeries(r.Context(), queue, time.Now().Add(-since))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"points": points})
}
