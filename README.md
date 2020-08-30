# Capstone Project - Technical Support
A company named **Super Technical Heros inc.** is selling high value technical products.
The Service and selling point of that company is that they take care about theire sold products.
They always sell a product with a Product-Lifetime Service contract.

In that given Project they want us to setup a Notification Pipeline.

## Table of Content
* [About the Dataset](#about)
* [Questions which should get possibly answered by the given Dataset](#goals)
* [Tables which gets create out of the given datasets](#new_tables)
* [About the Code](#code_info)
* [Write Up](#write_up)
    * [What's the goal?](#goal)
    * [Technologie Choices](#technology)
    * [Process](#process)
    * [Update Frequency](#update_frequency)
    * [Solving Problems](#solving_problems)


<a id="about"></a>

## About the Dataset
It's a real world example of a real world project (the target in that project were completely different).
The original dataset is about 7 tables with between 20 and 140 Columns.

I reduced that real world example for this usecase and use following Tables:
* **Equipments _71.950 Rows_** Columns: equipment_id, amount, contract_start_date, construction_year
* **Notifications _3.026.135 Rows_** Columns: priority, failure_start_date, failure_start_time, location, equipment_id

I have made some changes at the original dataset for beeing able to use it in this project.
Every ID got rearranged and some relevant data points as well.

I have created a Jupyter Notebook for giving you a first High Level Overview about the used dataset.

You can have a look to the included JSON in Directory `data`.
It's also on S3:
* `s3://capstone-project-janga/equipments.json`
* `s3://capstone-project-janga/notifications.json`


<a id="goals"></a>

## Questions which should get possibly answered by the given Dataset
* How many Notifications do we have in mean per day
* Which Location has the highest Notificationcount
* Which Equipment has the highest Notificationcount


<a id="new_tables"></a>

## Tables which gets create out of the given datasets
* **Locations** Columns: location, failure_start_date, notification_count
* **Equipment Notifications** Columns: equipment_id, amount, notification_count

It should be possible to add some BI Tools to that Database which gives a daily overview per Location.
This project should set up the Daily Data Pipeline by Using Airflow.
On top it's a Jupyter Notebook included which displays daily Informations of that data.

For a visalization of that schema you can take a look into the Schema.jpg file in that repo.

<a id="code_info"></a>

## About Code
The code was programmed in `Python v. 3.7.6`.
It follows mostly the `flake8` guidelines.


<a id="write_up"></a>

## Write Up


<a id="goal"></a>

### What's the Goal
There are more than 2 Million Notifications per year. Airflow is great for handeling the daily changes.
On that project I am interested in getting informations about performance per Equipment (does the age has influence about the notification count, does notifications have an effect about the amount, how does each region perform)
I wanted to give a "Mainstream" person the possibilities for making easy queries with Tools like Power BI. He should not have knowledge about Selects, Groupby and Joins. Such a person should simply working on tables which are easy to understand and easy to work with.


<a id="technology"></a>

### Technology Choices
**Databse: Postgres**
I do like to use Postgres more than Cassandra. If we want to look back historically a No SQL database like MongoDB or Cassandra could be better. But honestly: I would like to make some benchmarks for checking if that statement is really true.
I do enjoy working with SQL databases in case of data consistency. It's not kind of social Network where we need realtime informations to explore our data. It's okay for making a daily update which does not has to be incredible fast.
That's why I am fine with Postgres.

_Funfact: For our real world example we used Neo4J. We had some Contract informations and wanted to see if any contract property has an effect on notifications. Working with a Graph Database in this case was great. But in that example I have choosed it's completely overpowered._

**Pipeline: Airflow**
It's perfect for time triggered Events and modifying data.


<a id="process"></a>

### Process
You should be able to take a look at my Commitments. That should gives you an idea of how I worked.
Basically:
0. Preparing original data (I have made some changes at the real dataset. Changed ids, usecase and amount). This data comes from a payed project where I worked as a Data Scientist.
1. Creating Questions - what do I want to analyse? What kind of data do I need?
2. Creating Basic Database Schema
3. Make a High Level Data Evaluation in Jupyter Notebooks
4. Writing DAG Operators
5. Writing Queries for Fact tables
6. Setup the DAG Flow
7. Writing Summary of Readme


<a id="update_frequency"></a>

### Update frequency
In case of the given usecase it should be a daily Update. Each location should be able to make a daily check about theire notifications and compare themselfes to other locations.


<a id="solving_problems"></a>

### Solving Problems
1. _Data increases by 100x_ - I would try to distribute my dataset to different CPUs for transforming it. The given Dataset has about more than 3 Million Rows for 1.5 years. That are about more than 6000 inserts per day. It's already a daily update. The Pipeline have already a daily worktime period. A SQL Database should be able to handle 600000 Inserts per day and should be even able to analyse them. I would not make any changes at the current pipeline for this case.

2. _pipelines were run on a daily basis by 7am._ - Should be already the case.

3. That is already the case as well and in the current mode a kind of bottleneck. I would increase the CPU power for handeling more requests at the same time.