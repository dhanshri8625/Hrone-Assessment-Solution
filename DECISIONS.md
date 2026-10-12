# DECISIONS.md

1. **Indexes.** I added indexes so the database doesn't read every record. The most important one is on (emp_code, date). It makes sure an employee can't have two records for the same day, and it makes employee, monthly and trend queries fast. I added others for the sorted list, the status filter, the leaderboard and punch-out. I did not index late_minutes alone, because most records have different values and it would not help much.

2. **Punch-in race.** If two punch-ins arrive at the same moment, both try to save. The database accepts the first and rejects the second, and I return 409 for it. I don't check first and then save, because two requests can both pass the check.

3. **Ties.** If two people have the same late minutes, they get the same rank, and the next rank is skipped (1, 2, 2, 4). I apply the limit to the rank, so a tie at the cutoff shows everyone. That is why the list can be longer than the limit.

4. **Headcount.** I start from the employees, not from the attendance logs. This way, someone with no logs still counts. If I started from logs, they would disappear.

5. **100x data.** I would save daily totals for each employee and update them on every punch. Then reports read small summary data and don't scan millions of records.