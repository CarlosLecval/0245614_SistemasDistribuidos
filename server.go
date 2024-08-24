package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"sync"
)

type RequestRecord struct {
	BodyRecord Record `json:"record"`
}

type RequestOffset struct {
	Offset uint64 `json:"offset"`
}

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

type Log struct {
	mu      sync.Mutex
	records []Record
}

func (l *Log) AddRecord(record Record) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	record.Offset = uint64(len(l.records))
	l.records = append(l.records, record)

	return record.Offset
}

func (l *Log) ReadRecord(offset uint64) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.records) == 0 {
		return Record{}, errors.New("no records in log")
	}

	if offset >= uint64(len(l.records)) {
		return Record{}, errors.New("offset out of range")
	}

	return l.records[offset], nil
}

var log Log

func addToLogHandler(w http.ResponseWriter, r *http.Request) {
	var record RequestRecord
	err := json.NewDecoder(r.Body).Decode(&record)

	if err != nil {
		fmt.Println(err)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	offset := log.AddRecord(record.BodyRecord)

	response := map[string]uint64{"offset": offset}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func readFromLogHandler(w http.ResponseWriter, r *http.Request) {
	var offset RequestOffset
	err := json.NewDecoder(r.Body).Decode(&offset)

	if err != nil {
		fmt.Println(err)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	record, err := log.ReadRecord(offset.Offset)

	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	response := map[string]Record{"record": record}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func main() {
	router := mux.NewRouter()

	router.HandleFunc("/", addToLogHandler).Methods("POST")
	router.HandleFunc("/", readFromLogHandler).Methods("GET")
	http.ListenAndServe(":8080", router)
}
