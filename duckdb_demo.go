package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "fmt"
	"github.com/gorilla/mux"
	_ "github.com/marcboeker/go-duckdb"
	"log"
	"net/http"
	"strconv"
	"strings"
)

var db *sql.DB

// --- FIXED initDB (Final Version) ---
func initDB() *sql.DB {
	db, err := sql.Open("duckdb", "identifier.db")
	if err != nil {
		log.Fatal("Error opening database:", err)
	}

	// Drop existing table to start fresh
	_, err = db.Exec(`DROP TABLE IF EXISTS students;`)
	if err != nil {
		log.Fatal("Error dropping table:", err)
	}

	// FIX: Use the simple BIGINT PRIMARY KEY. We will manually manage the ID on INSERT.
	// The driver and your environment are rejecting BIGINT SERIAL and explicit sequences.
	_, err = db.Exec(`
        CREATE TABLE students (
           id BIGINT PRIMARY KEY, 
           name TEXT,
           age INTEGER,
           gpa FLOAT,
           organization_name TEXT
        );
    `)
	if err != nil {
		log.Fatal("Error creating table:", err)
	}

	// Create indexes (UNCHANGED)
	tryIndex := func(query string, name string) {
		if _, err := db.Exec(query); err != nil {
			errMsg := err.Error()
			if strings.Contains(errMsg, "already exists") || strings.Contains(errMsg, "Index with name") {
				log.Printf("Index %s already exists, skipping\n", name)
				return
			}
			log.Fatalf("Error creating index %s: %v", name, err)
		}
	}
	tryIndex("CREATE INDEX idx_students_org ON students (organization_name);", "idx_students_org")
	tryIndex("CREATE INDEX idx_students_age_gpa ON students (age, gpa);", "idx_students_age_gpa")
	tryIndex("CREATE INDEX idx_students_name ON students (name);", "idx_students_name")

	return db
}

func insertStudent(w http.ResponseWriter, r *http.Request) {
	var s struct {
		Name             string  `json:"name"`
		Age              int     `json:"age"`
		GPA              float64 `json:"gpa"`
		OrganizationName string  `json:"organization_name"`
	}

	if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.Name = strings.TrimSpace(s.Name)
	s.OrganizationName = strings.TrimSpace(s.OrganizationName)

	if s.Age < 0 || s.Age > 120 {
		http.Error(w, "Invalid age", http.StatusBadRequest)
		return
	}
	if s.GPA < 0.0 || s.GPA > 4.0 {
		http.Error(w, "Invalid GPA", http.StatusBadRequest)
		return
	}
	if s.OrganizationName == "" {
		s.OrganizationName = "No Organization"
	}

	// FIX: Manually calculate the next ID
	var newID int64
	// Find the current MAX(id) and add 1. COALESCE ensures it starts at 1 if the table is empty.
	err := db.QueryRow("SELECT COALESCE(MAX(id), 0) + 1 FROM students").Scan(&newID)
	if err != nil {
		log.Println("Failed to get next ID:", err)
		http.Error(w, "Database error: Failed to get next ID", http.StatusInternalServerError)
		return
	}

	// FIX: Include the 'id' column and the calculated newID in the INSERT
	_, err = db.Exec(`
    INSERT INTO students (id, name, age, gpa, organization_name)
    VALUES (?, ?, ?, ?, ?)
    `, newID, s.Name, s.Age, s.GPA, s.OrganizationName)

	if err != nil {
		log.Println("Insert failed:", err)
		http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Use the calculated ID for the response
	responseID := newID

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":      responseID,
		"message": "Student created successfully",
	})
}

// --- CRITICAL FINAL FIX: Using string formatting to bypass driver placeholder bug ---
// --- FINAL FIX: Update inside an Explicit Transaction ---
func updateStudent(w http.ResponseWriter, r *http.Request) {
	log.Println("UPDATE /students/{id} called (Final Attempt: Transaction)")

	idStr := mux.Vars(r)["id"]
	id, err := strconv.Atoi(idStr)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "Invalid student ID")
		return
	}

	var s struct {
		Name             string  `json:"name"`
		Age              int     `json:"age"`
		GPA              float64 `json:"gpa"`
		OrganizationName string  `json:"organization_name"`
	}

	if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
		jsonError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}

	// --- Validation & Pre-checks (Remains Unchanged) ---
	s.Name = strings.TrimSpace(s.Name)
	s.OrganizationName = strings.TrimSpace(s.OrganizationName)
	if s.Age < 0 || s.Age > 120 {
		jsonError(w, http.StatusBadRequest, "Age out of range")
		return
	}
	if s.GPA < 0.0 || s.GPA > 4.0 {
		jsonError(w, http.StatusBadRequest, "GPA out of range")
		return
	}
	if s.OrganizationName == "" {
		s.OrganizationName = "No Organization"
	}
	var exists int
	err = db.QueryRow("SELECT COUNT(*) FROM students WHERE id=?", id).Scan(&exists)
	if err != nil {
		log.Println("Check exists failed:", err)
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if exists == 0 {
		jsonError(w, http.StatusNotFound, "Student not found")
		return
	}
	// --- End Validation ---

	// 1. Begin Transaction
	tx, err := db.BeginTx(r.Context(), nil)
	if err != nil {
		log.Println("Failed to start transaction:", err)
		jsonError(w, http.StatusInternalServerError, "Database error: Could not start transaction")
		return
	}

	// Manual cleanup function for rollbacks
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r) // Re-throw panic
		}
	}()

	// 2. Build the safe SQL query string for execution inside the transaction
	safeName := strings.ReplaceAll(s.Name, "'", "''")
	safeOrg := strings.ReplaceAll(s.OrganizationName, "'", "''")

	query := fmt.Sprintf(
		`UPDATE students 
        SET 
            name = '%s', 
            age = %d, 
            gpa = %.2f, 
            organization_name = '%s' 
        WHERE 
            id = %d`,
		safeName, s.Age, s.GPA, safeOrg, id,
	)

	log.Printf("Executing query inside TX: %s", query)

	// 3. Execute the query using the transaction object
	result, err := tx.Exec(query)

	if err != nil {
		log.Println("Update failed inside TX:", err)
		tx.Rollback()
		jsonError(w, http.StatusInternalServerError, "Update failed: "+err.Error())
		return
	}

	// 4. Commit the transaction
	if err := tx.Commit(); err != nil {
		log.Println("Transaction commit failed:", err)
		jsonError(w, http.StatusInternalServerError, "Database error: Could not commit transaction")
		return
	}

	rowsAffected, _ := result.RowsAffected()
	log.Printf("Update successful, rows affected: %d", rowsAffected)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "Student updated successfully",
	})
}
func deleteStudent(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	_, err := db.Exec("DELETE FROM students WHERE id=?", id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func getStudents(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT id, name, age, gpa, organization_name FROM students")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var students []map[string]interface{}
	for rows.Next() {
		var id int
		var name, org string
		var age int
		var gpa float64
		rows.Scan(&id, &name, &age, &gpa, &org)
		students = append(students, map[string]interface{}{
			"id":                id,
			"name":              name,
			"age":               age,
			"gpa":               gpa,
			"organization_name": org,
		})
	}

	if students == nil {
		students = []map[string]interface{}{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(students)
}

func getOrganizations(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT DISTINCT organization_name FROM students WHERE organization_name != ''")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var orgs []string
	for rows.Next() {
		var org string
		rows.Scan(&org)
		orgs = append(orgs, org)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(orgs)
}

func filterStudents(w http.ResponseWriter, r *http.Request) {
	ageMinStr := r.URL.Query().Get("ageMin")
	ageMaxStr := r.URL.Query().Get("ageMax")
	gpaMinStr := r.URL.Query().Get("gpaMin")
	gpaMaxStr := r.URL.Query().Get("gpaMax")
	orgsStr := r.URL.Query().Get("organizations") // comma-separated org names

	// Parse numeric values safely
	ageMin, _ := strconv.Atoi(ageMinStr)
	ageMax, _ := strconv.Atoi(ageMaxStr)
	gpaMin, _ := strconv.ParseFloat(gpaMinStr, 64)
	gpaMax, _ := strconv.ParseFloat(gpaMaxStr, 64)

	// Base query
	query := "SELECT id, name, age, gpa, organization_name FROM students WHERE 1=1"
	args := []interface{}{}

	// Conditionally add filters
	if ageMinStr != "" && ageMaxStr != "" {
		query += " AND age BETWEEN ? AND ?"
		args = append(args, ageMin, ageMax)
	}
	if gpaMinStr != "" && gpaMaxStr != "" {
		query += " AND gpa BETWEEN ? AND ?"
		args = append(args, gpaMin, gpaMax)
	}
	if orgsStr != "" {
		orgs := strings.Split(orgsStr, ",")
		placeholders := make([]string, len(orgs))
		for i := range orgs {
			placeholders[i] = "?"
			args = append(args, orgs[i])
		}
		query += " AND organization_name IN (" + strings.Join(placeholders, ",") + ")"
	}

	log.Println("Filter params:", ageMinStr, ageMaxStr, gpaMinStr, gpaMaxStr, orgsStr)
	log.Println("Executing query:", query, "with args:", args)

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Println("Query failed:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type Student struct {
		ID               int     `json:"id"`
		Name             string  `json:"name"`
		Age              int     `json:"age"`
		GPA              float64 `json:"gpa"`
		OrganizationName string  `json:"organization_name"`
	}

	students := []Student{}
	for rows.Next() {
		var s Student
		if err := rows.Scan(&s.ID, &s.Name, &s.Age, &s.GPA, &s.OrganizationName); err != nil {
			log.Println("Scan failed:", err)
			http.Error(w, err.Error(), 500)
			return
		}
		students = append(students, s)
	}

	json.NewEncoder(w).Encode(students)
}

func searchStudentsByName(w http.ResponseWriter, r *http.Request) {
	name := "%" + r.URL.Query().Get("q") + "%"
	rows, err := db.Query(
		"SELECT id, name, age, gpa, organization_name FROM students WHERE name LIKE ?",
		name,
	)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()

	// scan & return JSON
}
func bulkInsertStudents(w http.ResponseWriter, r *http.Request) {
	var students []struct {
		Name string  `json:"name"`
		Age  int     `json:"age"`
		GPA  float64 `json:"gpa"`
		Org  string  `json:"organization_name"`
	}

	if err := json.NewDecoder(r.Body).Decode(&students); err != nil {
		jsonError(w, http.StatusBadRequest, "Invalid JSON body for bulk insert")
		return
	}

	// FIX: Get the starting ID before the transaction begins
	var currentMaxID int64
	err := db.QueryRow("SELECT COALESCE(MAX(id), 0) FROM students").Scan(&currentMaxID)
	if err != nil {
		http.Error(w, "Failed to get max ID for bulk insert", 500)
		return
	}
	nextID := currentMaxID + 1 // Start ID for the first new student

	tx, err := db.BeginTx(r.Context(), &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// FIX: Add 'id' to the prepared statement
	stmt, err := tx.Prepare(`
       INSERT INTO students (id, name, age, gpa, organization_name)
       VALUES (?, ?, ?, ?, ?)
    `)
	if err != nil {
		tx.Rollback()
		http.Error(w, err.Error(), 500)
		return
	}
	defer stmt.Close() // Close the statement when the transaction is done

	for _, s := range students {
		// FIX: Use the calculated and incremented ID
		_, err := stmt.Exec(nextID, s.Name, s.Age, s.GPA, s.Org)
		if err != nil {
			log.Println("Bulk insert failed for a row:", err)
			tx.Rollback()
			http.Error(w, "Transaction failed due to database error: "+err.Error(), 500)
			return
		}
		nextID++ // Increment the ID for the next student
	}

	if err := tx.Commit(); err != nil {
		log.Println("Transaction commit failed:", err)
		http.Error(w, "Transaction commit failed", 500)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "Bulk insert successful",
		"count":   strconv.Itoa(len(students)),
	})
}

func jsonError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{
		"error": msg,
	})
}
