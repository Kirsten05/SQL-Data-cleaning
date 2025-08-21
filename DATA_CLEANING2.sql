
-- Initial original table
SELECT*
FROM layoffs;

-- create a copy of the original table
CREATE TABLE layoffs_copy
LIKE layoffs;

SELECT*
FROM layoffs_copy;

-- insert data in the layoffs_copy table, with data from layoffs
INSERT layoffs_copy
SELECT*
FROM layoffs;


-- checking for duplicates in the dataset using ROW_NUMBER ( ), grouping rows by partitioning them based on the grouping of rows having common/same mentioned columns and numbering common rows in the grouping from 1
SELECT*,
ROW_NUMBER( ) OVER ( 
PARTITION BY company,location, industry,total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions) row_num
FROM layoffs_copy;

-- Creating a CTE to check if there are duplicates, ie if row_num > 1
WITH duplicate_cte AS (
	SELECT*,
	ROW_NUMBER( ) OVER ( 
	PARTITION BY company,location, industry,total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
	FROM layoffs_copy
)
SELECT *
FROM duplicate_cte
WHERE row_num >1;

-- checking if the duplicates returned are really the duplicates
SELECT*
FROM layoffs_copy
WHERE company = 'Oda'
;

-- from the above executed code we found out that they are not duplicates as they almost have the same data in almost every column but diff dates for some, or diff countries, 
-- so its important to partition by every column given in the table and not just a few to avoid such mistakes.
-- therefore we gonna correct our partition by above and partition by every column to find out if they are true duplicates existing with same everything in every single column
-- initially we didnt do so, so weare correcting our mistakes
-- mind you, its important to check if your duplicates returnrd are true duplicates to avoid deleting rows which arent duplicates, hence u gotta recheck and correct yourself if a mistake is done

-- recheck for true duplicates after correcting ourseleves
SELECT*
FROM layoffs_copy
WHERE company = 'Casper'
;

SELECT*
FROM layoffs_copy
WHERE company = 'Cazoo'
;
-- satisfied cuz yes, they're true duplicates

-- removing one of duplicates now
WITH duplicate_cte AS (
	SELECT*,
	ROW_NUMBER( ) OVER ( 
	PARTITION BY company,location, industry,total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
	FROM layoffs_copy
)
DELETE
FROM duplicate_cte
WHERE row_num >1;
-- we cant use delete, tho is the easiest cuz the cte table created isnt updatable, and delete command looks to change/update sth so it cant be done


-- 2nd option is creating a copy of the table layoffs_copy to be able to remove the duplicates
-- right click layoffs_copy, copy to cliboard, click create statement, then paste it
 CREATE TABLE `layoffs_copy2` (
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

-- check if the table is created
SELECT*
FROM layoffs_copy2;

-- insert data into the table
INSERT INTO layoffs_copy2
SELECT*,
ROW_NUMBER( ) OVER ( 
PARTITION BY company,location, industry,total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_copy;

SELECT *
FROM layoffs_copy2;

					-- TRYING TO INSERT DATA FROM THE CTE INSTEAD AND SEE WHAT HAPPENS NEXT
					/*INSERT INTO layoffs_copy2
					SELECT*
					FROM duplicate_cte;

					SELECT *
					FROM layoffs_copy2;*/
					-- it doesnt work because cte's are temp and u got to use them right after u create them which isnt the case here

-- back to dealing with duplicates
-- check for duplicates
SELECT *
FROM layoffs_copy2
WHERE row_num>1
;

-- DELETE THE DUPLICATES
DELETE
FROM layoffs_copy2
WHERE row_num>1
;

SELECT *
FROM layoffs_copy2
;

-- STANDARDIZING DATA
-- removing unwanted spaces 
SELECT company, TRIM(company)
FROM layoffs_copy2;

-- updating the comapny column
UPDATE layoffs_copy2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_copy2 
ORDER BY 1;
-- we come to see that there are duplicates in Crypto, CryptoCurrency, which mean the same thing

-- dealing with the duplicates in Crypto
SELECT *
FROM layoffs_copy2 
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_copy2 
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'; 

SELECT DISTINCT location
FROM layoffs_copy2
ORDER BY 1
;

-- dealing with country duplicates due to the formats they were written
SELECT  *
FROM layoffs_copy2
WHERE country LIKE 'United States%'
ORDER BY 1
;


-- or u could use trailing to remive the '.' character at the end of united states
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_copy2
ORDER BY 1;

UPDATE layoffs_copy2
SET country = 'United States'
WHERE country LIKE 'United States%'
;

-- standardizing the date column as it was initially in text format
SELECT `date`
FROM layoffs_copy2
;

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_copy2
;

UPDATE layoffs_copy2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')
;

-- alter the table so as to change the date column to date data type not text as it was previously
ALTER TABLE layoffs_copy2
MODIFY COLUMN `date` DATE;

-- Dealing with null or blank values
SELECT*
FROM layoffs_copy2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

SELECT*
FROM layoffs_copy2
WHERE industry IS NULL
OR industry=''
;

-- after running the previous command, we got a return on null/blank values under industry
-- wanna check if maybe one of the blank/null rows under company column, may have populated data under industry so as to fill the blanks and nulls
SELECT*
FROM layoffs_copy2
WHERE company = 'Airbnb'
; 		-- (found out there is a row where airbnb row is filled when it comes to industry)


-- do a self join to comapre the columns with are null/blank in the first table,
-- with the populated columns of the rows with exact company name in t2, so as to be able to populate the empty cells in t1 respectively
SELECT t1.industry, t2.industry
FROM layoffs_copy2 t1
JOIN layoffs_copy2 t2
	ON t2.company=t1.company
WHERE (t1.industry IS NULL OR t1.industry='')
AND t2.industry IS NOT NULL;

-- UPDATING MISSING VALUES IN THE TABLE
-- before updating for missing values, set all blank values to null first before updating it
UPDATE layoffs_copy2
SET industry = null
WHERE industry= '';

-- after updating blank values to null, that is when you populate data for Missing values in t1
UPDATE layoffs_copy2 t1
JOIN layoffs_copy2 t2
	ON t2.company=t1.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;


-- well, all values were filled under industry except for one (after agin checking for null values in industry), lets check for it and see
SELECT*
FROM layoffs_copy2
WHERE company LIKE 'Bally%'
; 
-- it didnt get populated as it doesnt have other multiple rows where the industry part was populated initially, hence remains null	

SELECT*
FROM layoffs_copy2;
-- found out there are still null values in total laid off and percentage laid off

-- check for the null values in both two columns
SELECT*
FROM layoffs_copy2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- then delete them
DELETE
FROM layoffs_copy2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- CHECK AGAIN
SELECT*
FROM layoffs_copy2;
-- found out that the row_num column is there. we gotta delete the column as we already solved for duplicates

-- dropping the column
ALTER TABLE layoffs_copy2
DROP COLUMN row_num;


-- CHECK AGAIN, and this our final clean data.
SELECT*
FROM layoffs_copy2;



    

