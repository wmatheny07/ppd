-- macros/wmo_weather_code.sql
-- Maps WMO weather codes to human-readable descriptions.
-- Reference: https://open-meteo.com/en/docs (weather_code)

{% macro wmo_weather_description(weather_code_column) %}
    case {{ weather_code_column }}
        when 0  then 'Clear sky'
        when 1  then 'Mainly clear'
        when 2  then 'Partly cloudy'
        when 3  then 'Overcast'
        when 45 then 'Fog'
        when 48 then 'Depositing rime fog'
        when 51 then 'Light drizzle'
        when 53 then 'Moderate drizzle'
        when 55 then 'Dense drizzle'
        when 56 then 'Light freezing drizzle'
        when 57 then 'Dense freezing drizzle'
        when 61 then 'Slight rain'
        when 63 then 'Moderate rain'
        when 65 then 'Heavy rain'
        when 66 then 'Light freezing rain'
        when 67 then 'Heavy freezing rain'
        when 71 then 'Slight snowfall'
        when 73 then 'Moderate snowfall'
        when 75 then 'Heavy snowfall'
        when 77 then 'Snow grains'
        when 80 then 'Slight rain showers'
        when 81 then 'Moderate rain showers'
        when 82 then 'Violent rain showers'
        when 85 then 'Slight snow showers'
        when 86 then 'Heavy snow showers'
        when 95 then 'Thunderstorm'
        when 96 then 'Thunderstorm with slight hail'
        when 99 then 'Thunderstorm with heavy hail'
        else 'Unknown'
    end
{% endmacro %}
