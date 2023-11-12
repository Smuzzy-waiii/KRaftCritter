package main

import (
	"encoding/gob"
	"fmt"
	"github.com/hashicorp/raft"
	"io"
	"strings"
)

// DistMap impl.  raft.FSM
type DistMap struct {
	distMap map[string]any
}

func (fsm *DistMap) Apply(l *raft.Log) interface{} {
	if (*fsm).distMap == nil {
		(*fsm).distMap = make(map[string]any)
	}

	data := string(l.Data)
	res := strings.Split(data, "|")
	key := res[0]
	val := res[1]
	fmt.Printf("[INFO] Setting key = %s | value = %s\n", key, val)
	(*fsm).distMap[key] = val
	//currently setting value to nil making it a hashtable
	return nil
}

func (fsm *DistMap) Snapshot() (raft.FSMSnapshot, error) {
	distMapCopy := make(map[string]any)
	for k := range fsm.distMap {
		distMapCopy[k] = fsm.distMap[k]
	}
	return &snapshot{
		distMapCopy,
	}, nil
}

func (fsm *DistMap) Restore(r io.ReadCloser) error {
	//b, err := ioutil.ReadAll(r)
	//if err != nil {
	//	return err
	//}

	d := gob.NewDecoder(r)
	err := d.Decode(&fsm.distMap)
	if err != nil {
		return err
	}
	return nil
}

type snapshot struct {
	distMap map[string]any
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	encoder := gob.NewEncoder(sink)
	err := encoder.Encode(s.distMap)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("sink.Write(): %v", err)
	}
	return sink.Close()
}

func (s *snapshot) Release() {

}
