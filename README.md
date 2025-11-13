# **ShopZada Data Warehouse Project**
### **Group: Rogue_One**

---

##  **Course Project Overview**
You are tasked to design and implement a complete **Data Warehouse (DWH)** solution for **ShopZada**, integrating multiple datasets into one centralized system.  
The project covers **architecture design, ETL workflows, analytics, and presentation** — all containerized and automated.

---

## ⚙️ **Main Tasks Summary**

### ** Architecture & Data Models**
**Goal:** Build the DWH foundation.

- Analyze source data & create data dictionary  
- Design **high-level architecture diagram** (Staging → Warehouse → Presentation)  
- Define **Kimball Star Schema** (Fact + Dimension tables)  
- Build **physical model** and ERD  
 Tools: Excel, diagrams.net, dbdiagram.io, DBeaver

---

### **Data Workflow Design & Implementation**
**Goal:** Create an automated, containerized ETL pipeline.

- Setup **Docker environment** (Postgres + Kestra)  
- Write **Python ingestion scripts** to load raw data  
- Develop **SQL transformation scripts** for DWH schema  
- Build **Kestra orchestration flow** (Ingest → Transform → Validate → Refresh Views)  
- Add **data quality checks** for nulls/duplicates  
Tools: Python, SQLAlchemy, Kestra, Docker

---

### **3️ Analytical Layer**
**Goal:** Build dashboards using DWH data.

- Define 3 key **business questions**  
- Create **analytical SQL views** (presentation layer)  
- Connect **Tableau** to PostgreSQL (in Docker)  
- Design interactive **dashboards** answering business questions  
Tools: SQL, Tableau

---

### ** Documentation & Presentation**
**Goal:** Package and present the entire project.

- Organize **GitHub repo** with proper folder structure  
- Write **technical documentation** (architecture, data dictionary, methods)  
- Prepare **10–15 slide presentation deck**  
- Conduct **live demo and defense**  
Tools: GitHub, Word/Docs, PowerPoint

---

### **Notes**
> Each member has assigned tasks but collaboration is open — just coordinate with whoever owns that part.    
> Final outputs: ETL + DWH + Dashboard + Docs + Presentation.

---

 ### **Rogue_One — Turning Data into Decisions.**
