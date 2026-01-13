/* ============================================================
   Project: Layoffs Dataset Cleaning
   Input : layoffs_data_cleaning (raw)
   Output: layoffs_clean (analysis-ready)

   Cleaning Steps:
     1) Copy raw -> staging (do not mutate raw)
     2) Remove duplicates (ROW_NUMBER)
     3) Standardize categories / text fields
     4) Handle nulls/blanks (industry fill)
     5) Remove unusable rows
     6) QA checks (counts, duplicates, null audit)
   ============================================================ */

SET SQL_SAFE_UPDATES = 0;
START TRANSACTION;

-- ------------------------------------------------------------
-- 0) Idempotent reruns
-- ------------------------------------------------------------
DROP TABLE IF EXISTS layoffs_staging;
DROP TABLE IF EXISTS layoffs_staging_dedup;
DROP TABLE IF EXISTS layoffs_clean;

-- ------------------------------------------------------------
-- 1) Stage raw data (copy)
-- ------------------------------------------------------------
CREATE TABLE layoffs_staging LIKE layoffs_data_cleaning;

INSERT INTO layoffs_staging
SELECT * FROM layoffs_data_cleaning;

-- ------------------------------------------------------------
-- 2) Build a typed staging table + deduplicate deterministically
-- ------------------------------------------------------------
CREATE TABLE layoffs_staging_dedup (
  company VARCHAR(255),
  location VARCHAR(255),
  industry VARCHAR(120),
  total_laid_off INT,
  percentage_laid_off VARCHAR(50),   -- keep as text first; can be converted later
  `date` VARCHAR(50),                -- raw text first; convert after standardization
  stage VARCHAR(120),
  country VARCHAR(120),
  funds_raised_millions INT,
  row_num INT NOT NULL
);

INSERT INTO layoffs_staging_dedup (
  company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
)
SELECT
  company,
  location,
  industry,
  total_laid_off,
  percentage_laid_off,
  `date`,
  stage,
  country,
  funds_raised_millions,
  ROW_NUMBER() OVER (
    PARTITION BY
      company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ORDER BY
      -- prefer non-null industry/metrics, then higher funds, then latest date (text for now)
      (industry IS NULL OR TRIM(industry) = ''),
      (total_laid_off IS NULL),
      (percentage_laid_off IS NULL OR TRIM(percentage_laid_off) = ''),
      funds_raised_millions DESC,
      `date` DESC
  ) AS rn
FROM layoffs_staging;

-- Remove duplicates
SELECT * 
FROM layoffs_staging_dedup
WHERE row_num > 1; 

DELETE FROM layoffs_staging_dedup
WHERE row_num > 1;

-- ------------------------------------------------------------
-- 3) Standardize text fields (trim, normalize categories)
-- ------------------------------------------------------------
-- Trim common text fields
UPDATE layoffs_staging_dedup
SET
  company  = TRIM(company),
  location = TRIM(location),
  industry = NULLIF(TRIM(industry), ''),
  stage    = NULLIF(TRIM(stage), ''),
  country  = NULLIF(TRIM(country), '');

-- Industry normalization: Crypto variants -> Crypto
SELECT DISTINCT industry
FROM layoffs_staging_dedup
ORDER BY 1;

UPDATE layoffs_staging_dedup
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Location fixes
SELECT DISTINCT location
FROM layoffs_staging_dedup
ORDER BY 1;

UPDATE layoffs_staging_dedup
SET location = 'Dusseldorf'
WHERE location LIKE '%sseldorf';

UPDATE layoffs_staging_dedup
SET location = 'Florianapolis'
WHERE location LIKE 'Florian%';

UPDATE layoffs_staging_dedup
SET location = 'Fredericton'
WHERE location LIKE 'Ferdericton';

UPDATE layoffs_staging_dedup
SET location = 'Malmo'
WHERE location LIKE 'Malm%';

-- Country normalization: remove trailing '.' then standardize United States
SELECT DISTINCT country
FROM layoffs_staging_dedup
ORDER BY 1;

UPDATE layoffs_staging_dedup
SET country = TRIM(TRAILING '.' FROM country)
WHERE country IS NOT NULL;

UPDATE layoffs_staging_dedup
SET country = 'United States'
WHERE country LIKE 'United States%';

-- ------------------------------------------------------------
-- 4) Date conversion + enforce DATE type via final table
-- ------------------------------------------------------------
-- Convert date text
UPDATE layoffs_staging_dedup
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- At this point, `date` column contains DATE values but is still VARCHAR in this table.
-- We'll store it properly as DATE in the final output table.

-- ------------------------------------------------------------
-- 5) Null handling for industry
-- ------------------------------------------------------------
-- Ensure blanks are NULL
UPDATE layoffs_staging_dedup
SET industry = NULL
WHERE industry = '';

/*
  Fill missing industry from another row.
  We use join on company + location to reduce incorrect fills.
*/
UPDATE layoffs_staging_dedup t1
JOIN layoffs_staging_dedup t2
  ON t1.company = t2.company
 AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;
  
SELECT *
FROM layoffs_staging_dedup
WHERE industry IS NULL 
OR industry = '';

-- ------------------------------------------------------------
-- 6) Remove unusable rows
-- ------------------------------------------------------------
DELETE FROM layoffs_staging_dedup
WHERE total_laid_off IS NULL
  AND (percentage_laid_off IS NULL OR TRIM(percentage_laid_off) = '');

-- ------------------------------------------------------------
-- 7) Final clean table (typed)
-- ------------------------------------------------------------
CREATE TABLE layoffs_clean (
  company VARCHAR(255),
  location VARCHAR(255),
  industry VARCHAR(120),
  total_laid_off INT,
  percentage_laid_off VARCHAR(50),
  `date` DATE,
  stage VARCHAR(120),
  country VARCHAR(120),
  funds_raised_millions INT
);

INSERT INTO layoffs_clean (
  company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
)
SELECT
  company,
  location,
  industry,
  total_laid_off,
  percentage_laid_off,
  CAST(`date` AS DATE),
  stage,
  country,
  funds_raised_millions
FROM layoffs_staging_dedup;

-- Optional: indexes for analytics
CREATE INDEX idx_layoffs_clean_date     ON layoffs_clean(`date`);
CREATE INDEX idx_layoffs_clean_company  ON layoffs_clean(company);
CREATE INDEX idx_layoffs_clean_country  ON layoffs_clean(country);
CREATE INDEX idx_layoffs_clean_industry ON layoffs_clean(industry);

-- ------------------------------------------------------------
-- 8) QA / Validation checks
-- ------------------------------------------------------------

-- Row counts: raw vs staging vs clean
SELECT
  (SELECT COUNT(*) FROM layoffs_data_cleaning) AS raw_rows,
  (SELECT COUNT(*) FROM layoffs_staging)       AS staging_rows,
  (SELECT COUNT(*) FROM layoffs_clean)         AS clean_rows;

-- Duplicate check (should return 0 rows)
SELECT
  company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
  COUNT(*) AS c
FROM layoffs_clean
GROUP BY 1,2,3,4,5,6,7,8,9
HAVING c > 1;

-- Null audit (quick health check)
SELECT
  SUM(company IS NULL OR company = '') AS null_company,
  SUM(`date` IS NULL)                  AS null_date,
  SUM(industry IS NULL)                AS null_industry,
  SUM(country IS NULL)                 AS null_country
FROM layoffs_clean;

COMMIT;
