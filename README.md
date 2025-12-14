# Data-engineering-DataWarehousingAndETLpipeline

Option 1: Data pipeline project

The Finnish Meteorological Institute (FMI) maintains a network of weather stations in Finland. 

Observations are updated about once in ten minutes and data contains basic weather information, like temperature and wind.

This is an example of accessing the most recent weather data using FMI Python API. Note that not all the weather stations are active. The list of stations here and in csv-file.

The goals of this project are:

    Collect weather data over the period of several days (there is no strict limit, you just need to have enough data to demonstrate that the pipeline works)
    Process the raw data into daily tables of observations from active weather stations
    Process daily data of selected stations into the time series of the most important parameters (temperature, humidity) from selected stations 
    Visualise the most recent observations and longer time series

Technical details of the ELT pipeline created in this project:

Data model and pipeline DAG plan

    Plan that includes all the steps of data processing

A Kafka data streaming tool that contains the following functionalities:

    Kafka producer that uses the FMI api, gets the most recent data from all the stations and sends it to Kafka
    Kafka consumer that receives the data and uploads it to BigQuery. It is probably better to upload BigQuery tables in bigger batches, like all the stations collected to a single dataframe, not message by message

Data processing in BigQuery, scheduled by Airflow or Mage 

    Daily processing of the data that has been updated to BigQuery
        Removing possible duplicate rows
        Investigate data quality, like missing data and possible outliers
        Save results to daily tables of weather data. Each table contains multiple observations (rows) from each station.
    After processing daily tables, update also long-term tables of selected stations (select at least five stations). Each of these five stations should have its own long-term table 

Note that you don't have to run the pipeline over several days. It is enough if you can demonstrate how it works. 
Visualization

    Visualize both daily and long-term observations. You can show for example the most recent records of the selected stations or map view of all the stations, and long-term line plots of those stations.  Note that the visualization should simple show that your pipeline works. It is not part of the evaluation this time. You can use Streamlit, Google Looker studio, PowerBI, or any other visualization tool. 
