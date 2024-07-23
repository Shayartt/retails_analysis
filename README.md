# retails_analysis
Retail Sales Analytics Platform project using :
  ○ Spark in Databricks (Scala) for ETL.
  ○ DBT (Data Build Tool): For data transformation and modeling.
  ○ Great Expectations: For data quality and validation.
  ○ Apache Superset: For data visualization and reporting.

Diagram Design (Will be updated into a better version soon) : 
![image](https://github.com/user-attachments/assets/3c7a5986-3316-415a-989c-39008004e45b)


<b> Projects Steps : </b> 
		1. Data Ingestion
			○ Collect sample retail sales data (e.g., transactions, product information, customer data) from open datasets or generate synthetic data.
			○ Use Spark to ingest this data into a Databricks environment.
		2. Data Processing with Spark & Databricks
			○ Use Spark within Databricks to clean and preprocess the data.
			○ Perform necessary transformations (e.g., calculating total sales, aggregating data by product category, etc.).
			○ Rename schema - db - table name to best practice : prod.bronze_retails.sales
		3. Data Modeling with DBT
			○ Set up a DBT project and connect it to your Databricks environment.
			○ Create DBT models to transform the preprocessed data into a star schema or other appropriate data models.
			○ Write SQL-based transformations in DBT and run them to create the final data models.
		4. Data Quality with Great Expectations
			○ Integrate Great Expectations into your DBT pipeline.
			○ Define and run data quality checks to ensure the integrity and accuracy of your transformed data.
			○ Configure alerts for data quality issues.
		5. Data Visualization with Apache Superset
			○ Set up Apache Superset and connect it to your Databricks/DBT output database.
			○ Create interactive dashboards and reports to visualize key metrics such as sales trends, customer demographics, and inventory levels.
			○ Use various chart types and visualizations to make the data insights clear and actionable.
		6. Final Integration and Automation
			○ Automate the data pipeline using Databricks jobs or another orchestrator.
			○ Schedule regular runs of your DBT models and Great Expectations checks.
			○ Update your Apache Superset dashboards to reflect the latest data.
	Deliverables
		• A Databricks notebook with Spark data processing code.
		• A DBT project with SQL-based transformation models.
		• Great Expectations configurations and results.
		• Apache Superset dashboards and reports.
	Learning Outcomes
		• Gain hands-on experience with DBT for data transformation and modeling.
		• Learn how to ensure data quality using Great Expectations.
		• Develop skills in creating interactive and insightful visualizations with Apache Superset.
    • Strengthen your existing knowledge of Spark and Databricks in a comprehensive data pipeline
