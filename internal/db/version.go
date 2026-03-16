package db

type Version struct {
	levels []level
}

func newVersionFromLevels(levels []level) *Version {
	return &Version{
		levels: cloneLevels(levels),
	}
}

func (v *Version) withLevels() *Version {
	if v == nil {
		return &Version{}
	}
	return &Version{
		levels: cloneLevels(v.levels),
	}
}

func cloneLevels(levels []level) []level {
	if levels == nil {
		return nil
	}

	out := make([]level, len(levels))
	for i := range levels {
		out[i] = level{
			id:      levels[i].id,
			dir:     levels[i].dir,
			l0Paths: append([]string(nil), levels[i].l0Paths...),
			runs:    append([]levelFile(nil), levels[i].runs...),
		}
	}
	return out
}
