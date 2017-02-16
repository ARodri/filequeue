package filequeue

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

type FileQueue struct {
	rChan chan Message

	queue []string
	qLock *sync.Mutex
	rCond *sync.Cond

	dataDir    string
	archiveDir string
	started    bool
	done       chan bool
}

type Message struct {
	Err error
	Msg []byte
}

func NewFileQueue(dataDir, archiveDir string) (*FileQueue, error) {
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		err := os.MkdirAll(dataDir, os.FileMode(int(0770)))
		if err != nil {
			return nil, fmt.Errorf("error creating data dir:", err)
		}
	}

	if _, err := os.Stat(archiveDir); os.IsNotExist(err) {
		err := os.MkdirAll(archiveDir, os.FileMode(int(0770)))
		if err != nil {
			return nil, fmt.Errorf("error creating archive dir:", err)
		}
	}

	files, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}
	sort.Sort(ByName(files))

	queue := []string{}

	for _, f := range files {
		if !f.IsDir() {
			queue = append(queue, f.Name())
		}
	}

	qLock := &sync.Mutex{}

	fq := FileQueue{
		rChan: make(chan Message),
		qLock: qLock,
		rCond: sync.NewCond(qLock),

		queue: queue,

		dataDir:    dataDir,
		archiveDir: archiveDir,
		started:    false,
		done:       make(chan bool),
	}
	fq.start()

	return &fq, nil
}

type ByName []os.FileInfo

func (a ByName) Len() int           { return len(a) }
func (a ByName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByName) Less(i, j int) bool { return a[i].Name() < a[j].Name() }

func (fq *FileQueue) Clear() error {
	fq.qLock.Lock()

	var err error
	files, err := ioutil.ReadDir(fq.dataDir)
	if err != nil {
		fmt.Printf("error reading dir %s:%s\n", fq.dataDir, err)
	} else {
		for _, file := range files {
			if err := os.Remove(filepath.Join(fq.dataDir, file.Name())); err != nil {
				err = fmt.Errorf("error removing file %s:%s\n", file.Name(), err)
			}
		}
	}

	fq.queue = []string{}
	fq.qLock.Unlock()
	return err
}

func (fq *FileQueue) start() {
	if !fq.started {
		// launch read routine
		go func(q *FileQueue) {
			done := false
			for !done {
				select {
				case <-q.done:
					done = true
				default:
					q.qLock.Lock()
					if len(q.queue) == 0 {
						q.rCond.Wait()
					}
					if len(q.queue) > 0 {
						item := q.queue[0]
						q.queue = q.queue[1:]
						q.qLock.Unlock()

						data, err := ioutil.ReadFile(filepath.Join(q.dataDir, item))
						msg := Message{}
						if err != nil {
							msg.Err = fmt.Errorf("error reading file:", err)
						} else {
							msg.Msg = data
							err = os.Rename(filepath.Join(q.dataDir, item), filepath.Join(q.archiveDir, item))
							if err != nil {
								msg.Err = fmt.Errorf("error removing file %s: %s\n", item, err)
							}
						}
						q.rChan <- msg
					} else {
						q.qLock.Unlock()
					}
				}
			}
			close(q.rChan)
			fq.started = false
		}(fq)
		fq.started = true
	}
}

func (fq *FileQueue) Write(msg []byte) {
	fq.qLock.Lock()
	msgFileName := strconv.Itoa(int(time.Now().UnixNano()))
	msgFile := filepath.Join(fq.dataDir, msgFileName)
	err := ioutil.WriteFile(
		msgFile,
		msg,
		os.FileMode(int(0660)),
	)
	if err != nil {
		fmt.Println("error writing file:", err)
	}
	fq.queue = append(fq.queue, msgFileName)
	fq.rCond.Signal()
	fq.qLock.Unlock()
}

func (fq *FileQueue) Read() <-chan Message {
	return fq.rChan
}

func (fq *FileQueue) Close() {
	close(fq.done)
}
