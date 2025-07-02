# Week 5: Data Pipeline Maintenance

## Team Members
- Alice
- Bob
- Charlie
- Dana

---

## 🔧 Pipeline Ownership

| Business Area | Pipeline                             | Primary Owner | Secondary Owner |
|---------------|--------------------------------------|---------------|-----------------|
| Profit        | Unit-level profit for experiments    | Alice         | Charlie         |
| Profit        | Aggregate profit (investors)         | Bob           | Dana            |
| Growth        | Aggregate growth (investors)         | Dana          | Alice           |
| Growth        | Daily growth for experiments         | Charlie       | Bob             |
| Engagement    | Aggregate engagement (investors)     | Alice         | Charlie         |

---

## 📞 On-Call Schedule

- On-call is rotated weekly.
- One engineer is on-call, one is backup.
- Backup takes over if primary is unavailable (due to holidays, sick leave, etc.)
- Schedule resets every 4 weeks.

| Week | On-Call | Backup  |
|------|---------|---------|
| 1    | Alice   | Bob     |
| 2    | Bob     | Charlie |
| 3    | Charlie | Dana    |
| 4    | Dana    | Alice   |
| 5    | Alice   | Bob     |
| ...  | ...     | ...     |

**Holidays**:
- During public holidays, backup takes over primary role.
- If both are unavailable, rotate to next person in list.

---

## 📚 Runbooks for Investor-Reported Pipelines

### 1. **Aggregate Profit Reported to Investors**

**Dependencies:**
- Orders database
- Product pricing feed
- Cost of Goods Sold (COGS) table

**What Could Go Wrong:**
- Upstream pricing table delays or errors
- COGS feed incomplete or contains nulls
- Incorrect currency conversion rates
- Aggregation job runs with outdated dimensions
- Metric drift due to schema changes

---

### 2. **Aggregate Growth Reported to Investors**

**Dependencies:**
- User registration events
- Monthly Active User (MAU) table
- Retention pipeline

**What Could Go Wrong:**
- MAU definition changes but not reflected in downstream logic
- Data lag in event ingestion (e.g., Kafka or event collector issues)
- Duplicate user IDs causing inflated numbers
- Scheduled job fails silently without alerts

---

### 3. **Aggregate Engagement Reported to Investors**

**Dependencies:**
- Session logs
- Event types (e.g., clicks, shares, time spent)
- User segmentation logic

**What Could Go Wrong:**
- Missing event logs due to upstream API or SDK failures
- Improper sessionization logic leading to incorrect metrics
- Anomaly detection suppressing valid outlier traffic
- Version drift in user segmentation rules

---

## ✅ Summary

- Owners assigned with redundancy
- Fair on-call rotation that accounts for holidays
- Investor-related pipelines have documented risks

This structure helps ensure our data pipelines are resilient, ownership is clear, and any issues can be triaged promptly.
