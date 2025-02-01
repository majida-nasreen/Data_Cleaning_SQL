-- Data Cleaning

select * 
from layoffs;

create table layoffs_staging 
like layoffs;

select * 
from layoffs_staging;

insert layoffs_staging 
select * 
from layoffs;

select *, 
row_number() over (
partition by company, location, industry, total_laid_off, percentage_laid_off, "date", stage, country, funds_raised_millions) as row_num 
from layoffs_staging;


with duplicate_cte as(
select *, 
row_number() over (
partition by company, location, industry, total_laid_off, percentage_laid_off, "date", stage, country, funds_raised_millions) as row_num 
from layoffs_staging)
select * from duplicate_cte where row_num>1;

select *
from layoffs_staging
where company = 'Oda';

with duplicate_cte as(
select *, 
row_number() over (
partition by company, location, industry, total_laid_off, percentage_laid_off, "date", stage, country, funds_raised_millions) as row_num 
from layoffs_staging)
delete 
from duplicate_cte where row_num>1;



create table layoffs_staging3(
company text, 
location text, 
industry text, 
total_laid_off int, 
percentage_laid_off text, 
date date, 
stage text, 
country text, 
funds_raised_millions int, 
row_num int);


insert into layoffs_staging3 
select *, row_number() over (partition by company, location, industry, total_laid_off, percentage_laid_off, date,
 stage, country, funds_raised_millions) as row_num from layoffs_staging;

select * from layoffs_staging3
where row_num>1;

delete
from layoffs_staging3
where row_num>1;


-- Standardizing Data


select company,trim(company) 
from layoffs_staging3;

update layoffs_staging3 
set company = trim(company);

select distinct industry 
from layoffs_staging3 order by 1;

select * from layoffs_staging3 
where industry like "Cripto"; 

update layoffs_staging3
set Industry = "Cripto"
where industry like "Cripto%";

select * from layoffs_staging3;

select distinct location 
from layoffs_staging3 
order by 1;

select distinct country,trim(trailing '.' from  country)
from layoffs_staging3 order by 1;

update layoffs_staging3 
set country = trim(trailing '.' from  country) 
where country like 'United States%';

select date , 
str_to_date (date, '%m/%d/%Y') 
from layoffs_staging3;

update layoffs_staging3 
set date = str_to_date (date, '%m/%d/%Y');

alter table layoffs_staging3
modify column date date;

select * 
from layoffs_staging3 
where total_laid_off is null
and percentage_laid_off is null;

update layoffs_staging3
set industry = null
where industry = ' ';

select distinct industry 
from layoffs_staging3 
where industry is null
or industry = ' ';

select * 
from layoffs_staging3 
where company="Airbnb";

select * 
from layoffs_staging3 t1 
join layoffs_staging3 t2 
on t1.company = t2.company 
where (t1.industry is null or t1.industry = ' ')
and t2.industry is not null;


select * 
from layoffs_staging3 
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging3 
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging3
drop column row_num;