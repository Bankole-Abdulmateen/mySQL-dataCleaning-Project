SELECT * FROM world_layoffs.layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

#---------------Cleaning data--------------------

SELECT *,
ROW_NUMBER () OVER 
(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging;

WITH duplicate_rows AS

(SELECT *,
ROW_NUMBER () OVER 
(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging
) 
SELECT *
FROM duplicate_rows
WHERE row_num > 1;


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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER () OVER 
(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

#--------Standardizing data------------------

SELECT company, trim(company)
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET company = trim(company);

SELECT *
FROM layoffs_staging2;

SELECT industry
FROM layoffs_staging2
WHERE industry LIKE "Crypto%";

UPDATE layoffs_staging2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%";

SELECT distinct industry
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2;

SELECT distinct country, trim(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = trim(TRAILING '.' FROM country)
WHERE country LIKE 'United states%';

SELECT distinct country
FROM layoffs_staging2
ORDER BY 1;

SELECT `date`, 
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;
#-------------------------
UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
AND t1.location = t2.location
WHERE t1.industry is null and t2.industry is not null;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry is null and t2.industry is not null;

SELECT *
FROM layoffs_staging2 
WHERE company = "AIRBNB";

SELECT *
FROM layoffs_staging2 
WHERE industry is null or industry = '';

SELECT *
FROM layoffs_staging2 
WHERE total_laid_off IS null and percentage_laid_off is null;


#------------DELETE ROWS-----
DELETE 
FROM layoffs_staging2 
WHERE total_laid_off IS null and percentage_laid_off is null;

#--------Removing the null column---------
ALTER TABLE layoffs_staging2
DROP COLUMN  row_num;

SELECT *
FROM layoffs_staging2; 

#-------------Exploratory Data Analysis----

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY 1
ORDER BY 2 DESC;

SELECT `date`
FROM layoffs_staging2;

WITH Rolling_total AS
(
SELECT substring(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE substring(`date`,1,7) IS NOT NULL
group by `MONTH`
ORDER BY 1 asc
)
SELECT `MONTH`, SUM(total_off) OVER(ORDER BY `MONTH`)
FROM Rolling_total;


SELECT company, year(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, year(`date`)
ORDER BY 3 desc;

WITH company_year (company, years, total_laid_off) AS
(SELECT company, year(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, year(`date`)
), Company_Year_Rank AS
(SELECT *, DENSE_RANK () OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranki
FROM company_year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE ranki <=5;