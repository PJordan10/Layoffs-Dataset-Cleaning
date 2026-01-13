# Data Cleaning: Global Layoffs Dataset (SQL)

## 📌 Project Overview

I took a raw, messy dataset of global tech layoffs (2020–2023) and transformed it into a reliable SQL database for analysis. The original data had duplicates, nulls, and inconsistent text that would have skewed any reporting.

## 🎯 The Challenge (The Mess)

Before cleaning, a simple SELECT SUM(total_laid_off) would have been wrong. The data contained:

* Duplicates: Same company, same day, listed multiple times.

* Inconsistent Naming: "Crypto", "CryptoCurrency", and "Crypto Currency" were treated as 3 different industries.

* Missing Data: Key fields like total_laid_off and percentage_laid_off were NULL in some rows.


## 🛠 My Approach (The Fix)

**1. Staging & Safety**

Created a staging table (layoffs_staging) to preserve the raw data. I never modify the source directly—a lesson learned from "silent failures."

**2. Deduplication with Window Functions**

Used ROW_NUMBER() over PARTITION BY (company, location, industry, date) to identify and delete [5] duplicate rows.

**3. Standardization**

Industry & Location Cleanup: Standardized variations (e.g., merged "Ferdericton" -> "Fredericton") using UPDATE and DISTINCT checks.

Date Parsing: Converted the text-based date column into a proper SQL DATE format using STR_TO_DATE.

**4. Handling NULLs & Logic**

Self-Join Population: Populated missing industry data by joining the table to itself. (e.g., If Airbnb had an industry listed in row A but was NULL in row B, I filled row B automatically).

Removal: Removed rows where both total_laid_off and percentage_laid_off were NULL, as they provided no analytical value.

## 🔍 The Result

**Original Row Count:** [2361]

**Final Row Count:** [1995]

**Outcome:** A clean, optimized MySQL table ready for Tableau/Power BI ingestion.
