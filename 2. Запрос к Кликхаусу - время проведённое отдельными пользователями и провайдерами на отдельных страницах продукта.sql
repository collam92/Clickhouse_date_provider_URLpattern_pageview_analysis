WITH
    '2024-08-15 00:00:00' AS start_datetime,
    '2024-08-15 23:59:59' AS end_datetime,
array(
    'Opportunity/#/opportunity/url_1/url_1_pattern_1$',
    'opportunity/#/opportunity/url_1/url_1_pattern_2$',
    'opportunity/#/opportunity/url_2/url_2_pattern_1$',
    'opportunity/#/opportunity/url_3/url_3_pattern_1$',
    'opportunity/#/opportunity/url_1/url_1_pattern_3$',
    'opportunity/#/opportunity/url_5$',
    'opportunity/#/opportunity/url_4/url_4_pattern_1',
    'opportunity/#/opportunity/url_6$',
    'opportunity/#/opportunity/url_1$',
    'opportunity/#/opportunity/url_7$',
    'opportunity/#/opportunity/url_2$',
    'opportunity/#/opportunity/url_8$',
    'opportunity/#/opportunity/url_4$',
    'opportunity/#/opportunity/url_3$',
    'opportunity/#/opportunity/url_9$',
    'opportunity/#/url_10$',
    'opportunity/#/url_11$'
) as pattern_array
    
SELECT providerId, userName, event_date, final_array.2 as pattern, toUInt64(arrayAvg(groupArray(final_array.1))) as avg_duration, 

toUInt64(if(length(arraySort(groupArray(final_array.1))) % 2 = 1,
    toFloat64(arraySort(groupArray(final_array.1))[intDiv(length(arraySort(groupArray(final_array.1))), 2) + 1]),  
    (toFloat64(arraySort(groupArray(final_array.1))[intDiv(length(arraySort(groupArray(final_array.1))), 2)]) + toFloat64(arraySort(groupArray(final_array.1))[intDiv(length(arraySort(groupArray(final_array.1))), 2) + 1])) / 2  
)) AS median_duration
 
FROM   
(SELECT 
    providerId,
    userName,
    userDateTime::date as event_date,
    arraySort(x -> x.2, groupArray((appName, toUnixTimestamp(userDateTime), pageUrl))) AS session_events,
  
            
            --- массив с pageUrl замененный на один из паттернов или на not_pattern
        arrayMap((event, match) -> (event.1, event.2, match), session_events, arrayMap(x -> if(multiMatchAny(x.3, pattern_array), 
        substring(pattern_array[multiMatchAnyIndex(x.3, pattern_array)], 1, length(pattern_array[multiMatchAnyIndex(x.3, pattern_array)]) - 1), 
        'not_pattern'), session_events)) AS replaced_url_session_events,

        
        --- массив с с событиями с разницей во времени
        arrayMap(i -> if(i = 1, (replaced_url_session_events[i].1, replaced_url_session_events[i].2, replaced_url_session_events[i].3, 0), 
            (replaced_url_session_events[i].1, replaced_url_session_events[i].2, replaced_url_session_events[i].3, 
            toUnixTimestamp(replaced_url_session_events[i].2) - toUnixTimestamp(replaced_url_session_events[i-1].2))),range(1, length(replaced_url_session_events))) AS replaced_url_session_events_with_time_diff,
    
        --- фильтр только тех событий, где разница во времени больше 5 секунд и паттерн один из указаных в первоначальном массиве
        arrayFilter(event -> event.4 >= 5 AND event.3 != 'not_pattern', replaced_url_session_events_with_time_diff) AS filtered_session_events,
        
        --- паттерн и продолжительность времени на нём
        arrayMap(event -> (event.4, event.3), filtered_session_events) AS final_array
        
            
        FROM 
            database_events_ru
        WHERE 
            userDateTime BETWEEN start_datetime AND end_datetime
            AND (event='page_view' or (event='struct' and sessionEventIndex='1' and appName='Product'))
            AND dictGetString('dbo.dictionary_providerattribute_from_tlw', 'ProviderStatus', ifNull(toUInt64OrNull(providerId), 0)) IN ('Живой', 'Отключенный')
            AND joinGet(dbo.users_from_accounts_for_join, 'superuser', userName, toInt32(1)) = 0 
            AND userIp NOT IN ('192.203.45.123', '10.1.2.3', '172.18.5.6', '192.168.2.4', '203.120.54.87', '88.44.33.99', '76.23.157.245') 
            AND dictGetInt32('dbo.dictionary_providerattribute_from_tlw', 'isTestName', ifNull(toUInt64OrNull(providerId), 0)) = 0
        GROUP BY 
            userName, providerId, userDateTime::date
        HAVING length(filtered_session_events) > 0
)
ARRAY JOIN final_array
GROUP by providerId, userName, event_date, pattern
    

