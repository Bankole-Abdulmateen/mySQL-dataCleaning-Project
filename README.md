# mySQL-dataCleaning-Project
A data cleaning process for a dataset related to company layoffs.
# Layoffs Data Cleaning and Preparation

## Project Overview
This project involves cleaning and preparing a dataset of company layoffs for analysis. The dataset includes attributes such as company name, location, industry, total layoffs, and more. The goal is to remove duplicates, standardize the data, and prepare it for insightful visualization.

## Key Features
- **Duplicate Detection**: Identified and removed duplicate rows based on key attributes using MySQL's `ROW_NUMBER()` function.
- **Data Staging**: Created staging tables for intermediate processing to preserve the original dataset.
- **Enhanced Data Quality**: Ensured clean, deduplicated data for reliable downstream analysis.

## Steps Performed
1. **Data Exploration**:
   - Queried and reviewed the structure and content of the raw data.
2. **Staging Table Creation**:
   - Created a copy of the original table for cleaning and transformation.
3. **Duplicate Removal**:
   - Used partitioning logic to identify duplicate rows and isolate them for review.
4. **Final Data Preparation**:
   - Prepared cleaned data in a new staging table (`layoffs_staging2`) with enhanced quality metrics.

## Tools Used
- **Database**: MySQL
- **Querying Techniques**: SQL Partitioning, Window Functions, Data Deduplication
