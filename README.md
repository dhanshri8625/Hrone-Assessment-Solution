# Employee Attendance & Analytics API

FastAPI + MongoDB. All code is in `app/main.py`.

## Run

Needs Python 3.11.4 and MongoDB 6.0+.

```bash
pip install -r requirements.txt
```

Set the database settings.

Windows PowerShell:
```powershell
$env:MONGO_URI="mongodb://localhost:27017"
$env:MONGO_DB="attendance_db"
```

Mac / Linux:
```bash
export MONGO_URI="mongodb://localhost:27017" MONGO_DB="attendance_db"
```

Start the API:
```bash
uvicorn app.main:app --port 8000
```

Indexes are created automatically at startup. Open `http://localhost:8000/docs` to try the endpoints.

## Notes

- MongoDB 6.0+ is needed because the analytics use `$setWindowFields`, `$densify` and `$lookup` with `localField` + `pipeline`.
- Analytics are MongoDB aggregation pipelines. The final half-up rounding is done in Python with `Decimal`, because MongoDB's `$round` rounds ties to even.
- Concurrency: employee create and punch-in use unique indexes. Punch-out uses a conditional update. PATCH uses compare-and-set with retries (409 if it keeps losing).