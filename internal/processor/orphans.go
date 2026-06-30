package processor

import (
	"io/fs"
	"path/filepath"
)

func findOrphans(root string, proc *processed) ([]string, error) {
	excluded := []string{}
	proc.mu.Lock()
	for _, c := range proc.items {
		excluded = append(excluded, c.Path())
	}
	proc.mu.Unlock()

	excludedSet := make(map[string]struct{}, len(excluded))
	for _, p := range excluded {
		// normalize so comparisons aren't tripped up by ./ vs absolute paths etc.
		abs, err := filepath.Abs(p)
		if err != nil {
			abs = p
		}
		excludedSet[abs] = struct{}{}
	}

	var missing []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		abs, aerr := filepath.Abs(path)
		if aerr != nil {
			abs = path
		}

		if _, found := excludedSet[abs]; !found {
			missing = append(missing, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return missing, nil
}
