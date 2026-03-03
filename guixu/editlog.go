package guixu

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
)

func appendEdit(dir string, e ManifestEdit) error {
	f, err := os.OpenFile(editLogPath(dir), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func replayEdits(dir string, m Manifest) (Manifest, error) {
	f, err := os.Open(editLogPath(dir))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return m, nil
		}
		return Manifest{}, err
	}
	defer func() { _ = f.Close() }()
	s := bufio.NewScanner(f)
	for s.Scan() {
		var e ManifestEdit
		if err := json.Unmarshal(s.Bytes(), &e); err != nil {
			return Manifest{}, err
		}
		if e.ID <= m.LastAppliedEdit {
			continue
		}
		applyEdit(&m, e)
		m.LastAppliedEdit = e.ID
	}
	if err := s.Err(); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

func applyEdit(m *Manifest, e ManifestEdit) {
	switch e.Type {
	case EditRotateVLog:
		m.ActiveVLog = e.VLogID
		m.VLogSegments = append(m.VLogSegments, e.VLogID)
	case EditAddL0Table:
		m.L0Tables = append(m.L0Tables, e.TableName)
	case EditDropL0Table:
		out := m.L0Tables[:0]
		for _, t := range m.L0Tables {
			if t != e.TableName {
				out = append(out, t)
			}
		}
		m.L0Tables = out
	case EditSetReadOnly:
		if e.ReadOnly != nil {
			m.ReadOnly = *e.ReadOnly
		}
	}
}
