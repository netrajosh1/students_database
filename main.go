package main

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func main() {
	db = initDB()
	defer db.Close() // Add this to properly close DB on shutdown

	router := mux.NewRouter()

	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Backend API running"))
	})

	// IMPORTANT: Specific routes MUST come BEFORE parameterized routes
	router.HandleFunc("/students/search", searchStudentsByName).Methods("GET")
	router.HandleFunc("/students/filter", filterStudents).Methods("GET")
	router.HandleFunc("/students/bulk", bulkInsertStudents).Methods("POST")
	router.HandleFunc("/organizations", getOrganizations).Methods("GET")

	// General CRUD routes
	router.HandleFunc("/students", getStudents).Methods("GET")
	router.HandleFunc("/students", insertStudent).Methods("POST")

	// Parameterized routes LAST (these will match anything)
	router.HandleFunc("/students/{id}", updateStudent).Methods("PUT")
	router.HandleFunc("/students/{id}", deleteStudent).Methods("DELETE")

	log.Println("Server running on http://localhost:8080")
	http.ListenAndServe(":8080", router)
}
