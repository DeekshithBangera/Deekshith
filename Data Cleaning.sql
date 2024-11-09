#Data Cleaning

SELECT * 
FROM layoffs;

-- 1.Staging the table
-- 2.Remove Duplicates if any
-- 3.Standardize and fix error in the data
-- 4.Work on blank or null values
-- 5.Remove any unnecessary rows or columns present

-- 1.Staging the table
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * FROM layoffs_staging;
INSERT INTO layoffs_staging
SELECT * FROM layoffs;

-- 2.Remove Duplicates if any
SELECT *,
ROW_NUMBER() OVER(PARTITION BY Company,industry,total_laid_off,percentage_laid_off,'date') AS row_num
FROM layoffs_staging;
-- Creating CTE duplicate_cte

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY Company,industry,total_laid_off,percentage_laid_off,`date`) AS row_num
FROM layoffs_staging
)

SELECT * FROM duplicate_cte
WHERE row_num>1;

-- let's just look at 'oda' to confirm
SELECT * FROM layoffs_staging
WHERE company='oda';
-- lookslike all three entries are unique and not duplicates
SELECT *
FROM (
SELECT *,
ROW_NUMBER() 
OVER(
PARTITION BY Company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
) as duplicates
WHERE row_num>1;

-- above we got actual dupllicate which need to be deleted 
-- we want to delete only those which has row_num >1 now
-- writing above code in CTE
WITH duplicate_cte AS 
(
SELECT *
FROM (
SELECT *,
ROW_NUMBER() 
OVER(
PARTITION BY Company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
) as duplicates
WHERE row_num>1
)
SELECT * FROM duplicate_cte
WHERE row_num>1;
DELETE
FROM duplicate_cte
WHERE row_num>1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2;
INSERT INTO layoffs_staging2 
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT *,
ROW_NUMBER() 
OVER(
PARTITION BY Company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT * FROM layoffs_staging2
WHERE row_num>1;
DELETE FROM 
layoffs_staging2
WHERE row_num>=2;
SELECT * FROM layoffs_staging2 ;

-- 2. Standardize Data
SELECT company FROM layoffs_staging2 ;
-- looks like there are some white spaces in comapny
SELECT company,TRIM(company) FROM layoffs_staging2;
UPDATE layoffs_staging2
SET company=TRIM(company);
-- lets look at industry
SELECT Distinct(industry) FROM layoffs_staging2 order by Industry;
-- There null ,blank which need to be addressed and Crypto,crypocurrency,Crypto currency are the same needto be standardised

SELECT *
FROM layoffs_staging2
WHERE industry Like 'Crypto%';

UPDATE layoffs_staging2
SET industry='Crypto'
WHERE industry Like 'Crypto%';

SELECT Distinct(country) FROM layoffs_staging2 order by 1;
-- Country Unted states has spelling error can be corrected
SELECT *
FROM layoffs_staging2
WHERE country Like 'United states';

UPDATE layoffs_staging2
SET Country='United states'
WHERE country Like 'United states%';
-- Another Method
-- UPDATE layoffs_staging2
-- SET country = TRIM(TRAILING '.' FROM country);

-- Change Datatypeof date
SELECT `date` 
FROM layoffs_staging2;

SELECT `date` ,STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date`=STR_TO_DATE(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;
-- 4.Work on blank or null values
SELECT distinct(industry)
FROM layoffs_staging2 order by 1;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
or industry='';
UPDATE layoffs_staging2
SET industry=NULL
WHERE industry='';
-- lets check airbnb to see if we can populate this industry data 
SELECT *
FROM layoffs_staging2
WHERE Company = 'airbnb';

SELECT *
FROM layoffs_staging2 as t1
JOIN layoffs_staging2 as t2
	ON t1.company=t2.company
		AND t1.location=t2.location
WHERE (t1.industry IS NULL OR t1.industry='') 
	AND t2.industry IS NOT NULL;
    
    
Update layoffs_staging2 t1
JOIN layoffs_staging2 as t2
	ON t1.company=t2.company
SET t1.industry=t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;
    
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
or industry='';

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

-- we dont need information where percentage_laid_off and total_laid_off arre null
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;
-- 5.Remove any unnecessary rows or columns present
SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT *
FROM layoffs_staging;












