# REVIEW.md

| # | Area | Problem | Fix |
|---|---|---|---|
| 1 | MongoDB dates | Times could depend on the computer's time zone. | Used UTC and converted to IST when needed. |
| 2 | Employee creation | Duplicate employee codes could be saved during simultaneous requests. | Added a unique index and return 409 for duplicates. |
| 3 | Employee validation | Invalid emails, dates, shifts, and employee codes could be accepted. | Added input validation. |
| 4 | Pagination | Page numbers and page sizes were not handled correctly. | Fixed pagination and added limits. |
| 5 | Health check | The API returned OK without checking MongoDB. | Added a database ping and return 503 if it fails. |
| 6 | Punch-in | Unknown employees and duplicate punch-ins could cause problems. | Added error handling and a unique attendance index. |
| 7 | Attendance calculations | Late minutes, work hours, and overtime could be calculated incorrectly. | Fixed time-zone handling, rounding, and overtime rules. |
| 8 | Attendance listing | All records were loaded into Python, which could be slow with large data. | Added database filtering, sorting, and pagination. |
| 9 | API responses | Some responses included internal IDs or incorrectly formatted dates. | Standardized the response fields and timestamps. |
| 10 | Database indexes | Queries could scan too many records. | Added indexes and checked query performance with explain tests. |

**Other checks:** I also reviewed environment variable loading, database client usage, and employee response formatting. These were generally fine, although some related validation and error-handling issues needed fixing.