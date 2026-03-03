package guixu

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
)

const (
	manifestFileName = "MANIFEST.v2.json"
	editLogFileName  = "EDITLOG.v2.jsonl"
)

// Manifest keeps the latest engine snapshot.
type Manifest struct {
	Version         int      `json:"version"`
	Epoch           uint64   `json:"epoch"`
	ReadOnly        bool     `json:"read_only"`
	ActiveVLog      uint64   `json:"active_vlog"`
	VLogSegments    []uint64 `json:"vlog_segments"`
	L0Tables        []string `json:"l0_tables"`
	Seq             uint64   `json:"seq"`
	LastAppliedEdit uint64   `json:"last_applied_edit"`
}

// EditType is an append-only manifest delta type.
type EditType string

const (
	EditRotateVLog  EditType = "rotate_vlog"
	EditAddL0Table  EditType = "add_l0_table"
	EditDropL0Table EditType = "drop_l0_table"
	EditSetReadOnly EditType = "set_read_only"
)

// ManifestEdit is a durable delta event.
type ManifestEdit struct {
	ID        uint64   `json:"id"`
	Type      EditType `json:"type"`
	VLogID    uint64   `json:"vlog_id,omitempty"`
	TableName string   `json:"table_name,omitempty"`
	ReadOnly  *bool    `json:"read_only,omitempty"`
}

func manifestPath(dir string) string { return filepath.Join(dir, manifestFileName) }
func editLogPath(dir string) string  { return filepath.Join(dir, editLogFileName) }

func readManifest(dir string) (Manifest, bool, error) {
	b, err := os.ReadFile(manifestPath(dir))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Manifest{}, false, nil
		}
		return Manifest{}, false, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return Manifest{}, false, err
	}
	if m.Version <= 0 {
		return Manifest{}, false, errors.New("guixu: invalid manifest version")
	}
	sort.Slice(m.VLogSegments, func(i, j int) bool { return m.VLogSegments[i] < m.VLogSegments[j] })
	return m, true, nil
}

func writeManifest(dir string, m Manifest) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp := manifestPath(dir) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, manifestPath(dir))
}
