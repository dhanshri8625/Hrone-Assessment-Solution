"""
Employee Attendance & Analytics API  (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB  (a local .env is loaded, but real environment variables win)

Layout of this file:
  1. config + Mongo client        5. attendance endpoints
  2. time / rounding helpers      6. analytics endpoints (aggregation pipelines)
  3. business rules R1-R5         7. explain endpoint
  4. models + employees
"""
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from datetime import date as date_cls
from datetime import datetime, time as dtime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, IndexModel, MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError

load_dotenv()  # never overrides variables that are already set in the environment
log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. Config
# --------------------------------------------------------------------------- #
# tz_aware=True: PyMongo returns UTC-aware datetimes instead of naive ones.
client = MongoClient(
    os.environ.get("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,
    serverSelectionTimeoutMS=5000,
)
db = client[os.environ.get("MONGO_DB", "attendance_db")]

# Index names are fixed so queries can `hint` them (the planner can't pick a COLLSCAN by mistake).
IX_LOG_EMP_DATE = "emp_date_unique"
IX_LOG_DATE_EMP = "date_emp"
IX_LOG_STATUS = "status_date_emp"
IX_LOG_PUNCH = "emp_punch_in"
IX_EMP_DEPT_JOINED = "dept_joined"


def ensure_indexes() -> None:
    """Idempotent: create_indexes is a no-op when an identical index already exists."""
    db.employees.create_indexes(
        [
            IndexModel([("emp_code", ASCENDING)], name="emp_code_unique", unique=True),
            IndexModel([("department", ASCENDING), ("emp_code", ASCENDING)], name="dept_emp"),
            IndexModel([("department", ASCENDING), ("joined_on", ASCENDING)], name=IX_EMP_DEPT_JOINED),
        ]
    )
    db.attendance_logs.create_indexes(
        [
            IndexModel([("emp_code", ASCENDING), ("date", ASCENDING)], name=IX_LOG_EMP_DATE, unique=True),
            IndexModel([("date", DESCENDING), ("emp_code", ASCENDING)], name=IX_LOG_DATE_EMP),
            IndexModel(
                [("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], name=IX_LOG_STATUS
            ),
            IndexModel([("emp_code", ASCENDING), ("punch_in", DESCENDING)], name=IX_LOG_PUNCH),
        ]
    )


@asynccontextmanager
async def lifespan(_: FastAPI):
    deadline = time.time() + 15
    while True:
        try:
            ensure_indexes()
            break
        except PyMongoError as exc:  # Mongo may still be starting; /health reports 503 until it is up
            if time.time() > deadline:
                log.error("could not create indexes: %s", exc)
                break
            time.sleep(1)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


@app.exception_handler(PyMongoError)
async def mongo_error_handler(_: Request, exc: PyMongoError):
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})


# --------------------------------------------------------------------------- #
# 2. Time + rounding helpers
# --------------------------------------------------------------------------- #
IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
EPOCH_MIN = 100_000_000_000
EPOCH_MAX = 4_102_444_800_000
PRESENCE = ["PRESENT", "WFH", "ON_DUTY"]
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]
DATE_PATTERN = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
MONTH_PATTERN = r"^[0-9]{4}-(0[1-9]|1[0-2])$"
HHMM_PATTERN = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"
EpochMs = Annotated[int, Field(strict=True, ge=EPOCH_MIN, le=EPOCH_MAX)]


def ms_to_dt(ms: int) -> datetime:
    """Epoch ms -> aware UTC datetime, truncated to whole seconds (R1)."""
    return EPOCH + timedelta(seconds=ms // 1000)


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    if dt.tzinfo is None:  # defensive: seeded/naive values are UTC by contract
        dt = dt.replace(tzinfo=UTC)
    delta = dt - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000


def now_ms_precision() -> datetime:
    n = datetime.now(UTC)
    return n.replace(microsecond=(n.microsecond // 1000) * 1000)  # Mongo stores milliseconds


def trunc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.replace(microsecond=0)


def half_up(value: Any, places: int) -> float:
    """Round half-up (not banker's) using the shortest decimal repr of the value."""
    q = Decimal(1).scaleb(-places)
    return float(Decimal(str(value)).quantize(q, rounding=ROUND_HALF_UP))


def check_date(value: str, name: str = "date") -> date_cls:
    if not re.fullmatch(DATE_PATTERN, value):
        raise HTTPException(422, f"{name} must be YYYY-MM-DD")
    try:
        return date_cls.fromisoformat(value)
    except ValueError:
        raise HTTPException(422, f"{name} is not a valid calendar date")


def month_bounds(month: str):
    """-> (first_str, next_month_first_str, last_str, first_date, last_date)"""
    y, m = int(month[:4]), int(month[5:7])
    try:
        first = date_cls(y, m, 1)
        nxt = date_cls(y + 1, 1, 1) if m == 12 else date_cls(y, m + 1, 1)
    except ValueError:
        raise HTTPException(422, "month is out of range")
    last = nxt - timedelta(days=1)
    return first.isoformat(), nxt.isoformat(), last.isoformat(), first, last


def hhmm(s: str) -> int:
    h, m = s.split(":")
    return int(h) * 60 + int(m)


# --------------------------------------------------------------------------- #
# 3. Business rules R1-R5
# --------------------------------------------------------------------------- #
def is_overnight(shift_start: str, shift_end: str) -> bool:
    return hhmm(shift_end) <= hhmm(shift_start)


def attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> str:
    """R1: IST date of the punch-in; an overnight shift's early-morning punch-in belongs to the day before."""
    ist = punch_in.astimezone(IST)
    d = ist.date()
    if is_overnight(shift_start, shift_end):
        seconds_of_day = ist.hour * 3600 + ist.minute * 60 + ist.second
        if seconds_of_day < hhmm(shift_end) * 60:
            d -= timedelta(days=1)
    return d.isoformat()


def _at(d: date_cls, hhmm_str: str) -> datetime:
    return datetime.combine(d, dtime(hour=int(hhmm_str[:2]), minute=int(hhmm_str[3:])), tzinfo=IST)


def late_minutes_for(punch_in: datetime, shift_start: str, date_str: str) -> int:
    """R2: late only when strictly more than 10:00 after shift start; value = floor minutes since shift start."""
    secs = int((trunc(punch_in) - _at(date_cls.fromisoformat(date_str), shift_start)).total_seconds())
    return secs // 60 if secs > 600 else 0


def overtime_for(punch_out: datetime, shift_start: str, shift_end: str, date_str: str) -> int:
    """R3: floor minutes after shift end, counted only if >= 30. Overnight shifts end on the next day."""
    d = date_cls.fromisoformat(date_str)
    end = _at(d + timedelta(days=1) if is_overnight(shift_start, shift_end) else d, shift_end)
    secs = int((trunc(punch_out) - end).total_seconds())
    minutes = secs // 60
    return minutes if minutes >= 30 else 0


def work_hours_and_half_day(punch_in: datetime, punch_out: datetime):
    """R4 + R5: exact Decimal arithmetic, half-up to 2 places; half day when rounded value < 4.50."""
    secs = int((trunc(punch_out) - trunc(punch_in)).total_seconds())
    wh = (Decimal(secs) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return float(wh), wh < Decimal("4.50")


def derive(emp: dict, status: str, punch_in: Optional[datetime], punch_out: Optional[datetime], date_str: str) -> dict:
    """All derived fields for a record, from its final inputs."""
    zero = {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    if status not in PRESENCE or punch_in is None:
        return zero
    out = dict(zero, late_minutes=late_minutes_for(punch_in, emp["shift_start"], date_str))
    if punch_out is not None:
        out["work_hours"], out["half_day"] = work_hours_and_half_day(punch_in, punch_out)
        out["overtime_minutes"] = overtime_for(punch_out, emp["shift_start"], emp["shift_end"], date_str)
    return out


# --------------------------------------------------------------------------- #
# Serialisation (Mongo document -> API JSON)
# --------------------------------------------------------------------------- #
def _hist_value(key: str, v: Any):
    return dt_to_ms(v) if key in ("punch_in", "punch_out") and isinstance(v, datetime) else v


def record_out(d: dict) -> dict:
    history = []
    for h in d.get("history") or []:
        changes = {
            k: {"from": _hist_value(k, c.get("from")), "to": _hist_value(k, c.get("to"))}
            for k, c in (h.get("changes") or {}).items()
        }
        history.append({"at": dt_to_ms(h.get("at")), "by": h.get("by"), "reason": h.get("reason"), "changes": changes})
    return {
        "emp_code": d["emp_code"],
        "date": d["date"],
        "status": d["status"],
        "punch_in": dt_to_ms(d.get("punch_in")),
        "punch_out": dt_to_ms(d.get("punch_out")),
        "work_hours": d.get("work_hours"),
        "late_minutes": d.get("late_minutes") or 0,
        "overtime_minutes": d.get("overtime_minutes") or 0,
        "half_day": bool(d.get("half_day", False)),
        "history": history,
    }


def employee_out(d: dict) -> dict:
    return {
        "emp_code": d["emp_code"],
        "name": d["name"],
        "email": d["email"],
        "department": d["department"],
        "shift_start": d["shift_start"],
        "shift_end": d["shift_end"],
        "joined_on": d["joined_on"],
        "created_at": dt_to_ms(d.get("created_at")),
    }


def get_employee(emp_code: str) -> dict:
    emp = db.employees.find_one({"emp_code": emp_code}, {"_id": 0})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


# --------------------------------------------------------------------------- #
# 4. Health + employees
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(503, "database unavailable")
    return {"status": "ok"}


class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field("09:30", pattern=HHMM_PATTERN)
    shift_end: str = Field("18:30", pattern=HHMM_PATTERN)
    joined_on: str = Field(pattern=DATE_PATTERN)

    @field_validator("joined_on")
    @classmethod
    def _real_date(cls, v: str) -> str:
        date_cls.fromisoformat(v)  # ValueError -> 422
        return v

    @model_validator(mode="after")
    def _shift_differs(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = now_ms_precision()
    try:
        db.employees.insert_one(doc)  # the unique index on emp_code decides, not a prior read
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    q = {"department": department} if department else {}
    total = db.employees.count_documents(q)
    cur = db.employees.find(q, {"_id": 0}).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    return {"items": [employee_out(d) for d in cur], "total": total, "page": page, "page_size": page_size}


# --------------------------------------------------------------------------- #
# 5. Attendance
# --------------------------------------------------------------------------- #
class PunchInIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else trunc(now_ms_precision())
    d = attendance_date(ts, emp["shift_start"], emp["shift_end"])
    doc = {
        "emp_code": body.emp_code,
        "date": d,
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        **derive(emp, body.status, ts, None, d),
        "history": [],
    }
    try:
        db.attendance_logs.insert_one(doc)  # unique (emp_code, date): exactly one concurrent insert wins
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee(body.emp_code)
    out_dt = ms_to_dt(body.punched_at) if body.punched_at is not None else trunc(now_ms_precision())
    latest = list(
        db.attendance_logs.find({"emp_code": body.emp_code, "punch_in": {"$lte": out_dt}})
        .sort("punch_in", DESCENDING)
        .hint(IX_LOG_PUNCH)
        .limit(1)
    )
    rec = latest[0] if latest else None
    if rec is None:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    p_in = trunc(rec["punch_in"])
    if out_dt <= p_in:
        raise HTTPException(422, "punched_at must be after punch_in")
    if out_dt - p_in > timedelta(hours=24):
        raise HTTPException(422, "punched_at is more than 24 hours after punch_in")
    fields = derive(emp, rec["status"], p_in, out_dt, rec["date"])
    # Filter on punch_out being empty: of two simultaneous punch-outs only one can match.
    updated = db.attendance_logs.find_one_and_update(
        {"emp_code": rec["emp_code"], "date": rec["date"], "punch_out": None},
        {"$set": {"punch_out": out_dt, **fields}},
        return_document=True,
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return record_out(updated)


class RegularizeIn(BaseModel):
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def _no_explicit_nulls(self):
        for f in ("status", "punch_in", "punch_out"):
            if f in self.model_fields_set and getattr(self, f) is None:
                raise ValueError(f"{f} cannot be null")
        return self


HISTORY_EMPTY = {"$or": [{"history": {"$size": 0}}, {"history": {"$exists": False}}]}


@app.patch("/attendance/{emp_code}/{date}")
def regularize(body: RegularizeIn, emp_code: str, date: str = Path(pattern=DATE_PATTERN)):
    check_date(date)
    emp = get_employee(emp_code)
    for _ in range(5):  # optimistic concurrency: retry if someone changed the record under us
        rec = db.attendance_logs.find_one({"emp_code": emp_code, "date": date})
        if rec is None:
            raise HTTPException(404, "no attendance record for that date")

        old_status = rec["status"]
        old_in, old_out = rec.get("punch_in"), rec.get("punch_out")
        new_status = body.status or old_status

        if new_status in ("ABSENT", "LEAVE"):
            if body.punch_in is not None or body.punch_out is not None:
                raise HTTPException(422, "ABSENT/LEAVE cannot carry punch times")
            new_in = new_out = None
        else:
            new_in = ms_to_dt(body.punch_in) if body.punch_in is not None else old_in
            new_out = ms_to_dt(body.punch_out) if body.punch_out is not None else old_out
            if new_in is None:
                raise HTTPException(422, "a presence status requires punch_in")
            new_in = trunc(new_in)
            new_out = trunc(new_out) if new_out is not None else None
            if body.punch_in is not None and attendance_date(new_in, emp["shift_start"], emp["shift_end"]) != date:
                raise HTTPException(422, "punch_in must stay on the record's attendance date")
            if new_out is not None and not (new_in < new_out <= new_in + timedelta(hours=24)):
                raise HTTPException(422, "punch_out must be after punch_in and within 24 hours")

        old_in_n = trunc(old_in) if old_in is not None else None
        old_out_n = trunc(old_out) if old_out is not None else None
        if (new_status, new_in, new_out) == (old_status, old_in_n, old_out_n):
            raise HTTPException(422, "request changes nothing")

        new_vals = {"status": new_status, "punch_in": new_in, "punch_out": new_out,
                    **derive(emp, new_status, new_in, new_out, date)}
        old_vals = {"status": old_status, "punch_in": old_in, "punch_out": old_out,
                    "work_hours": rec.get("work_hours"), "late_minutes": rec.get("late_minutes") or 0,
                    "overtime_minutes": rec.get("overtime_minutes") or 0, "half_day": bool(rec.get("half_day", False))}
        changes = {}
        for k, new_v in new_vals.items():
            old_v = old_vals[k]
            if k in ("punch_in", "punch_out"):
                old_v = trunc(old_v) if old_v is not None else None
            if old_v != new_v:
                changes[k] = {"from": old_vals[k], "to": new_v}

        entry = {"at": now_ms_precision(), "by": body.regularized_by, "reason": body.reason, "changes": changes}
        n_hist = len(rec.get("history") or [])
        hist_clause = HISTORY_EMPTY if n_hist == 0 else {"history": {"$size": n_hist}}
        flt = {"emp_code": emp_code, "date": date, "status": old_status,
               "punch_in": old_in, "punch_out": old_out, **hist_clause}
        updated = db.attendance_logs.find_one_and_update(
            flt, {"$set": new_vals, "$push": {"history": entry}}, return_document=True
        )
        if updated is not None:
            return record_out(updated)
    raise HTTPException(409, "record was modified concurrently, please retry")


def attendance_query(emp_code, date_from, date_to, status, page, page_size):
    """Single place that builds the list query, so /attendance and the explain endpoint stay identical."""
    flt: dict = {}
    if emp_code:
        flt["emp_code"] = emp_code
    if date_from or date_to:
        flt["date"] = {}
        if date_from:
            flt["date"]["$gte"] = date_from
        if date_to:
            flt["date"]["$lte"] = date_to
    if status:
        flt["status"] = status
    sort = {"date": -1, "emp_code": 1}
    hint = IX_LOG_EMP_DATE if emp_code else (IX_LOG_STATUS if status else IX_LOG_DATE_EMP)
    return flt, sort, (page - 1) * page_size, page_size, hint


def _validate_list_filters(date_from, date_to):
    f = check_date(date_from, "date_from") if date_from else None
    t = check_date(date_to, "date_to") if date_to else None
    if f and t and f > t:
        raise HTTPException(422, "date_from must not be after date_to")


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    _validate_list_filters(date_from, date_to)
    flt, sort, skip, limit, hint = attendance_query(emp_code, date_from, date_to, status, page, page_size)
    total = db.attendance_logs.count_documents(flt)
    cur = db.attendance_logs.find(flt, {"_id": 0}).sort(list(sort.items())).skip(skip).limit(limit).hint(hint)
    return {"items": [record_out(d) for d in cur], "total": total, "page": page, "page_size": page_size}


# --------------------------------------------------------------------------- #
# 6. Analytics (all aggregation pipelines)
# --------------------------------------------------------------------------- #
def _weekday():
    return {"$isoDayOfWeek": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d", "timezone": "UTC"}}}


def _is_present():
    return {"$in": ["$status", PRESENCE]}


def _present_value():
    return {"$cond": [{"$eq": ["$half_day", True]}, 0.5, 1]}  # missing half_day -> false -> 1


def _present_days_expr():
    """R5-R7: present statuses on Mon-Fri only; half day counts 0.5."""
    return {"$sum": {"$cond": [{"$and": [_is_present(), {"$lte": [_weekday(), 5]}]}, _present_value(), 0]}}


def _count_if(cond):
    return {"$sum": {"$cond": [cond, 1, 0]}}


def _round_half_up_expr(x, places):
    f = 10 ** places
    # +1e-9 absorbs binary float error so exact decimal ties (0.12345) round up
    return {"$divide": [{"$floor": {"$add": [{"$multiply": [x, f]}, 0.5 + 1e-9]}}, f]}


def monthly_pipeline(emp_code: str, month: str) -> list:
    first, nxt, _, _, _ = month_bounds(month)
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first, "$lt": nxt}}},
        {"$group": {
            "_id": None,
            "present_days": _present_days_expr(),
            "leave_days": _count_if({"$eq": ["$status", "LEAVE"]}),
            "late_count": _count_if({"$gt": ["$late_minutes", 0]}),
            "total_late_minutes": {"$sum": "$late_minutes"},
            "total_overtime_minutes": {"$sum": "$overtime_minutes"},
        }},
    ]


def working_days(first: date_cls, last: date_cls, joined_on: str) -> int:
    """R7: Mon-Fri in the month, counted from joined_on for a mid-month joiner."""
    start = max(first, date_cls.fromisoformat(joined_on))
    if start > last:
        return 0
    return sum(1 for i in range((last - start).days + 1) if (start + timedelta(days=i)).weekday() < 5)


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: str = Query(pattern=MONTH_PATTERN)):
    _, _, _, first, last = month_bounds(month)
    emp = get_employee(emp_code)
    res = list(db.attendance_logs.aggregate(monthly_pipeline(emp_code, month), hint=IX_LOG_EMP_DATE))
    agg = res[0] if res else {}
    present = float(agg.get("present_days", 0))
    wd = working_days(first, last, emp["joined_on"])
    pct = half_up(Decimal(str(present)) / Decimal(wd) * 100, 2) if wd > 0 else None
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": wd,
        "present_days": present,
        "leave_days": int(agg.get("leave_days", 0)),
        "late_count": int(agg.get("late_count", 0)),
        "total_late_minutes": int(agg.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(agg.get("total_overtime_minutes", 0)),
        "attendance_pct": pct,
    }


def dept_summary_pipeline(month: str, department: Optional[str]) -> list:
    """Starts from employees (so people with no logs still count) and joins each one's month of logs."""
    first, nxt, last, _, _ = month_bounds(month)
    emp_match: dict = {"joined_on": {"$lte": last}}  # R9
    if department:
        emp_match["department"] = department
    has_wh = {"$and": [_is_present(), {"$isNumber": "$work_hours"}]}
    per_employee = [  # runs inside $lookup, once per employee, on the (emp_code, date) index
        {"$match": {"date": {"$gte": first, "$lt": nxt}}},
        {"$group": {
            "_id": None,
            "present_days": _present_days_expr(),
            "wh_cents": {"$sum": {"$cond": [has_wh, {"$round": [{"$multiply": ["$work_hours", 100]}, 0]}, 0]}},
            "wh_count": _count_if(has_wh),
            "late_count": _count_if({"$gt": ["$late_minutes", 0]}),
            "total_late_minutes": {"$sum": "$late_minutes"},
            "leave_count": _count_if({"$eq": ["$status", "LEAVE"]}),
            "on_duty_count": _count_if({"$eq": ["$status", "ON_DUTY"]}),
        }},
    ]

    def first_of(field):
        return {"$sum": {"$ifNull": [{"$arrayElemAt": [f"$logs.{field}", 0]}, 0]}}

    return [
        {"$match": emp_match},
        {"$lookup": {"from": "attendance_logs", "localField": "emp_code", "foreignField": "emp_code",
                     "pipeline": per_employee, "as": "logs"}},
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            "present_days": first_of("present_days"),
            "wh_cents": first_of("wh_cents"),
            "wh_count": first_of("wh_count"),
            "late_count": first_of("late_count"),
            "total_late_minutes": first_of("total_late_minutes"),
            "leave_count": first_of("leave_count"),
            "on_duty_count": first_of("on_duty_count"),
        }},
        {"$sort": {"_id": 1}},
    ]


@app.get("/analytics/departments/summary")
def department_summary(month: str = Query(pattern=MONTH_PATTERN), department: Optional[str] = None):
    month_bounds(month)
    rows = db.employees.aggregate(dept_summary_pipeline(month, department), hint=IX_EMP_DEPT_JOINED)
    items = []
    for r in rows:
        avg = None
        if r["wh_count"] > 0:  # average over records, not over per-employee averages
            avg = half_up(Decimal(int(r["wh_cents"])) / Decimal(int(r["wh_count"])) / 100, 2)
        items.append({
            "department": r["_id"],
            "headcount": int(r["headcount"]),
            "present_days": float(r["present_days"]),
            "avg_work_hours": avg,
            "late_count": int(r["late_count"]),
            "total_late_minutes": int(r["total_late_minutes"]),
            "leave_count": int(r["leave_count"]),
            "on_duty_count": int(r["on_duty_count"]),
        })
    return {"month": month, "items": items}


def leaderboard_pipeline(month: str, department: Optional[str], limit: int) -> list:
    first, nxt, _, _, _ = month_bounds(month)
    pipe: list = [
        {"$match": {"date": {"$gte": first, "$lt": nxt}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"},
                    "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},  # logs of unknown employees disappear here
    ]
    if department:
        pipe.append({"$match": {"emp.department": department}})  # rank only inside the department
    pipe += [
        # $rank = standard competition ranking: ties share a rank, the next rank is skipped (1,2,2,4)
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies to the rank, after ranking
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$project": {"_id": 0, "rank": 1, "emp_code": "$_id", "name": "$emp.name",
                      "department": "$emp.department", "total_late_minutes": 1, "late_count": 1}},
    ]
    return pipe


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: str = Query(pattern=MONTH_PATTERN),
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    month_bounds(month)
    items = list(db.attendance_logs.aggregate(leaderboard_pipeline(month, department, limit), hint=IX_LOG_DATE_EMP))
    for it in items:
        it["rank"], it["total_late_minutes"], it["late_count"] = int(it["rank"]), int(it["total_late_minutes"]), int(it["late_count"])
    return {"month": month, "items": items}


def trend_pipeline(codes: list, joined: list, date_from: date_cls, date_to: date_cls) -> list:
    lo = datetime(date_from.year, date_from.month, date_from.day, tzinfo=UTC)
    hi = datetime(date_to.year, date_to.month, date_to.day, tzinfo=UTC) + timedelta(days=1)  # bounds: upper is exclusive
    rate = _round_half_up_expr({"$divide": ["$present_count", "$headcount"]}, 4)
    return [
        {"$match": {"emp_code": {"$in": codes}, "date": {"$gte": date_from.isoformat(), "$lte": date_to.isoformat()}}},
        {"$group": {
            "_id": "$date",
            "present_count": {"$sum": {"$cond": [_is_present(), _present_value(), 0]}},
            "late_count": _count_if({"$gt": ["$late_minutes", 0]}),
        }},
        {"$addFields": {"d": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d", "timezone": "UTC"}}}},
        # gap filling: one row for every calendar day in the range, weekends and empty days included
        {"$densify": {"field": "d", "range": {"step": 1, "unit": "day", "bounds": [lo, hi]}}},
        {"$addFields": {
            "date": {"$dateToString": {"date": "$d", "format": "%Y-%m-%d"}},
            "present_count": {"$ifNull": ["$present_count", 0]},
            "late_count": {"$ifNull": ["$late_count", 0]},
            "is_working_day": {"$lte": [{"$isoDayOfWeek": "$d"}, 5]},
        }},
        {"$addFields": {"headcount": {"$size": {"$filter": {
            "input": {"$literal": joined}, "as": "j", "cond": {"$lte": ["$$j", "$date"]}}}}}},  # R9, per day
        {"$addFields": {"attendance_rate": {"$cond": [
            {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]}, rate, None]}}},
        # trailing 7-row window inside the requested range; $avg skips null rates (weekends)
        {"$setWindowFields": {"sortBy": {"d": 1},
                              "output": {"ma": {"$avg": "$attendance_rate", "window": {"documents": [-6, 0]}}}}},
        {"$addFields": {"moving_avg_7d": _round_half_up_expr("$ma", 4)}},  # null stays null
        {"$sort": {"d": 1}},
        {"$project": {"_id": 0, "date": 1, "is_working_day": 1, "headcount": 1, "present_count": 1,
                      "late_count": 1, "attendance_rate": 1, "moving_avg_7d": 1}},
    ]


def trend_inputs(department: str, from_: str, to: str):
    f, t = check_date(from_, "from"), check_date(to, "to")
    if t < f:
        raise HTTPException(422, "to must not be before from")
    if (t - f).days + 1 > 92:
        raise HTTPException(422, "range exceeds 92 days")
    emps = list(db.employees.find({"department": department}, {"_id": 0, "emp_code": 1, "joined_on": 1}))
    if not emps:
        raise HTTPException(404, "unknown department")
    return trend_pipeline([e["emp_code"] for e in emps], sorted(e["joined_on"] for e in emps), f, t)


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str, from_: str = Query(alias="from"), to: str = Query()):
    pipe = trend_inputs(department, from_, to)
    items = list(db.attendance_logs.aggregate(pipe, hint=IX_LOG_EMP_DATE))
    for it in items:
        it["headcount"], it["late_count"] = int(it["headcount"]), int(it["late_count"])
        it["present_count"] = float(it["present_count"])
    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# 7. Explain
# --------------------------------------------------------------------------- #
ExplainEndpoint = Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"]


def _need(**kw):
    missing = [k for k, v in kw.items() if v is None]
    if missing:
        raise HTTPException(422, f"missing query parameter(s): {', '.join(missing)}")


@app.get("/admin/explain/{endpoint}")
def explain(
    endpoint: ExplainEndpoint,
    emp_code: Optional[str] = None,
    month: Optional[str] = Query(None, pattern=MONTH_PATTERN),
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    # Each branch builds the command with the same builder function the real endpoint uses.
    if endpoint == "attendance_list":
        _validate_list_filters(date_from, date_to)
        flt, sort, skip, lim, hint = attendance_query(emp_code, date_from, date_to, status, page, page_size)
        coll = "attendance_logs"
        cmd = {"find": coll, "filter": flt, "projection": {"_id": 0}, "sort": sort,
               "skip": skip, "limit": lim, "hint": hint}
    else:
        if endpoint == "employee_monthly":
            _need(emp_code=emp_code, month=month)
            month_bounds(month)
            coll, pipe, hint = "attendance_logs", monthly_pipeline(emp_code, month), IX_LOG_EMP_DATE
        elif endpoint == "department_summary":
            _need(month=month)
            month_bounds(month)
            coll, pipe, hint = "employees", dept_summary_pipeline(month, department), IX_EMP_DEPT_JOINED
        elif endpoint == "late_leaderboard":
            _need(month=month)
            month_bounds(month)
            coll, pipe, hint = "attendance_logs", leaderboard_pipeline(month, department, limit), IX_LOG_DATE_EMP
        else:  # department_trend
            _need(department=department, **{"from": from_, "to": to})
            coll, pipe, hint = "attendance_logs", trend_inputs(department, from_, to), IX_LOG_EMP_DATE
        cmd = {"aggregate": coll, "pipeline": pipe, "cursor": {}, "hint": hint}
    raw = db.command({"explain": cmd, "verbosity": "executionStats"})
    return {"endpoint": endpoint, "collection": coll,
            "explain": json.loads(json_util.dumps(raw, json_options=json_util.RELAXED_JSON_OPTIONS))}
