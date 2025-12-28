
use role accountadmin;
use schema quickstart_prod.silver;


CREATE OR REPLACE FUNCTION get_city_for_airport(iata VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'main'
AS
$$
from snowflake.snowpark.files import SnowflakeFile
from _snowflake import vectorized
import pandas
import json

@vectorized(input=pandas.DataFrame)
def main(df):
    airport_list = json.loads(
        SnowflakeFile.open("@bronze.raw/airport_list.json", 'r', require_scoped_url = False).read()
    )
    airports = {airport[3]: airport[1] for airport in airport_list}
    return df[0].apply(lambda iata: airports.get(iata.upper()))
$$;



 CREATE OR REPLACE VIEW flight_emissions AS
 select 
            departure_airport, 
            arrival_airport, 
            avg(estimated_co2_total_tonnes / seats) * 1000 as co2_emissions_kg_per_person
        from oag_flight_emissions_data_sample.public.estimated_emissions_schedules_sample
        where seats != 0 and estimated_co2_total_tonnes is not null
        group by departure_airport, arrival_airport;
 

 
 CREATE OR REPLACE VIEW flight_punctuality AS 
select 
            departure_iata_airport_code, 
            arrival_iata_airport_code, 
            count(
                case when arrival_actual_ingate_timeliness IN ('OnTime', 'Early') THEN 1 END
            ) / COUNT(*) * 100 as punctual_pct
        from oag_flight_status_data_sample.public.flight_status_latest_sample
        where arrival_actual_ingate_timeliness is not null
        group by departure_iata_airport_code, arrival_iata_airport_code;
 
 CREATE OR REPLACE VIEW  flights_from_home AS
 select 
            departure_airport, 
            arrival_airport, 
            get_city_for_airport(arrival_airport) arrival_city,  
            co2_emissions_kg_per_person, 
            punctual_pct,
        from flight_emissions
        join flight_punctuality 
            on departure_airport = departure_iata_airport_code 
            and arrival_airport = arrival_iata_airport_code
        where departure_airport = (
            select $1:airport 
            from @quickstart_common.public.quickstart_repo/branches/main/data/home.json 
                (FILE_FORMAT => bronze.json_format));

 CREATE OR REPLACE VIEW  weather_forecast AS 
    select 
            postal_code, 
            avg(avg_temperature_air_2m_f) avg_temperature_air_f, 
            avg(avg_humidity_relative_2m_pct) avg_relative_humidity_pct, 
            avg(avg_cloud_cover_tot_pct) avg_cloud_cover_pct, 
            avg(probability_of_precipitation_pct) precipitation_probability_pct
        from global_weather__climate_data_for_bi.standard_tile.forecast_day
        where country = 'US'
        group by postal_code;
   
CREATE OR REPLACE VIEW  major_us_cities AS  
select 
            geo.geo_id, 
            geo.geo_name, 
            max(ts.value) total_population
        from SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.DATACOMMONS_TIMESERIES ts
        join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_INDEX geo 
            on ts.geo_id = geo.geo_id
        join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_RELATIONSHIPS geo_rel 
            on geo_rel.related_geo_id = geo.geo_id
        where true
            and ts.variable_name = 'Total Population, census.gov'
            and date >= '2010-01-01'
            and geo.level = 'City'
            and geo_rel.geo_id = 'country/USA'
            and value > 100000
        group by geo.geo_id, geo.geo_name
        order by total_population desc;

CREATE OR REPLACE VIEW  zip_codes_in_city AS   
  select 
            city.geo_id city_geo_id, 
            city.geo_name city_geo_name, 
            city.related_geo_id zip_geo_id, 
            city.related_geo_name zip_geo_name
        from SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_RELATIONSHIPS country
        join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_RELATIONSHIPS city 
            on country.related_geo_id = city.geo_id
        where true
            and country.geo_id = 'country/USA'
            and city.level = 'City'
            and city.related_level = 'CensusZipCodeTabulationArea'
        order by city_geo_id;

CREATE OR REPLACE VIEW  weather_joined_with_major_cities AS   
        select 
            city.geo_id, 
            city.geo_name, 
            city.total_population,
            avg(avg_temperature_air_f) avg_temperature_air_f,
            avg(avg_relative_humidity_pct) avg_relative_humidity_pct,
            avg(avg_cloud_cover_pct) avg_cloud_cover_pct,
            avg(precipitation_probability_pct) precipitation_probability_pct
        from major_us_cities city
        join zip_codes_in_city zip on city.geo_id = zip.city_geo_id
        join weather_forecast weather on zip.zip_geo_name = weather.postal_code
        group by city.geo_id, city.geo_name, city.total_population;

--Added additional code for testing
CREATE OR REPLACE VIEW attractions AS
     select
        city.geo_id,
        city.geo_name,
        count(case when category_main = 'Aquarium' THEN 1 END) aquarium_cnt,
        count(case when category_main = 'Zoo' THEN 1 END) zoo_cnt,
        count(case when category_main = 'Korean Restaurant' THEN 1 END) korean_restaurant_cnt,
    from SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.POINT_OF_INTEREST_INDEX poi
    join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.POINT_OF_INTEREST_ADDRESSES_RELATIONSHIPS poi_add 
        on poi_add.poi_id = poi.poi_id
    join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.US_ADDRESSES address 
        on address.address_id = poi_add.address_id
    join major_us_cities city on city.geo_id = address.id_city
    where true
        and category_main in ('Aquarium', 'Zoo', 'Korean Restaurant')
        and id_country = 'country/USA'
    group by city.geo_id, city.geo_name;

    