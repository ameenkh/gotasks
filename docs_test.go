package gotasks_test

// Doc-rot guard: symbols removed from the API must not survive in code
// comments, docs or examples. PLAN.md is exempt — it is the dated
// historical record and names removed symbols on purpose.
//
// When you remove or rename public API, add the old name here.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var removedSymbols = []string{
	// store options
	"WithRetentionByType", "WithRetention(", "WithCollection(",
	"WithQueueClaimIndex",
	// manager options
	"WithMaxBatch", "WithQueueLeaseTime", "WithFinalizeBatch",
	"WithReapInterval", "WithoutHeartbeat", "WithMetrics(",
	// enqueue options (replaced by TaskPolicy)
	"WithRunAt", "WithDelay(", "WithUniqueKey", "WithMaxAttempts", "WithTTL(",
	// removed concepts
	"WithAllQueues", "DefaultQueue", "NoExpiry", "StatusFailed",
	"locked_until", "locked_by", "LockedUntil", "LockedBy",
	"gotasks/ui\"",
}

func TestDocsReferenceNoRemovedSymbols(t *testing.T) {
	err := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() {
			if name == ".git" || name == ".github" {
				return filepath.SkipDir
			}
			return nil
		}
		if name == "PLAN.md" || name == "docs_test.go" {
			return nil
		}
		if !strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, ".md") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(data), "\n") {
			for _, sym := range removedSymbols {
				if strings.Contains(line, sym) {
					t.Errorf("%s:%d references removed symbol %q: %s",
						path, i+1, sym, strings.TrimSpace(line))
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
