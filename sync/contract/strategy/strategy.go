package strategy

import (
	"strings"
)

//Strategy represents a sync strategy
type Strategy struct {
	Chunk            Chunk
	IDColumns        []string
	Columns          []string
	Diff             Diff
	DirectAppend     bool   `description:"if this flag is set all insert/append data is stream directly to the dest table"`
	MergeStyle       string `description:"supported value:merge,insertReplace,insertUpdate,insertDelete"`
	PartitionConf    PartitionConf
	AppendOnly       bool `description:"if set instead of merge, insert will be used"`
	AppendUpdateOnly bool `description:"if set instead of merge, insert will be used"`
	Force            bool `description:"if set skip checks if values in sync"`
}

//Clone clones strategy
func (s *Strategy) Clone() *Strategy {
	return &Strategy{
		Chunk:            s.Chunk,
		IDColumns:        s.IDColumns,
		Diff:             s.Diff,
		DirectAppend:     s.DirectAppend,
		Columns:          s.Columns,
		MergeStyle:       s.MergeStyle,
		PartitionConf:    s.PartitionConf,
		AppendOnly:       s.AppendOnly,
		AppendUpdateOnly: s.AppendUpdateOnly,
		Force:            s.Force,
	}
}

//IDColumn returns IDColumn
func (s *Strategy) IDColumn() string {
	if len(s.IDColumns) == 1 {
		return s.IDColumns[0]
	}
	return ""
}

//Init initializes strategy
func (s *Strategy) Init() error {
	err := s.Diff.Init()
	if err == nil {
		if err = s.PartitionConf.Init(); err == nil {
			err = s.Chunk.Init()
		}
	}
	return err
}

//IsOptimized returns true if optimized sync
func (s *Strategy) IsOptimized() bool {
	return !s.Force
}

//UseUpperCaseSQL update id, partition column to upper case
func (r *Strategy) UseUpperCaseSQL() {
	if len(r.IDColumns) > 0 {
		for i, v := range r.IDColumns {
			r.IDColumns[i] = strings.ToUpper(v)
		}
	}
	if len(r.PartitionConf.Columns) > 0 {
		for i, v := range r.PartitionConf.Columns {
			r.PartitionConf.Columns[i] = strings.ToUpper(v)
		}
	}
	if len(r.Diff.Columns) > 0 {
		for i := range r.Diff.Columns {
			r.Diff.Columns[i].Name = strings.ToUpper(r.Diff.Columns[i].Name)
			r.Diff.Columns[i].Alias = strings.ToUpper(r.Diff.Columns[i].Alias)
		}
	}
}
