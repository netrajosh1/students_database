# CS348 Project (Students Database) - Stage 3 Demo

Small Go backend (Gorilla Mux) + React frontend.

## Repo layout
- `stage3-demo/` - Go backend service
- `frontend/` - React app
- `main.go` - backend router and API entrypoint

## Requirements
- Go (1.20+ recommended)
- Node.js & npm
- PostgreSQL-compatible database, used DuckDB (or other SQL DB used by the project)

## Environment
Set the database connection string before running the backend, e.g.:
```bash
export DATABASE_URL="postgres://user:pass@localhost:5432/dbname?sslmode=disable"
