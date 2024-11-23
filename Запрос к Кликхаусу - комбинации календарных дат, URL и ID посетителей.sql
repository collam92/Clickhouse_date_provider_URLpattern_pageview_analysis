WITH 
    -- Период времени
    '2024-08-01' AS start_date,  -- Начальная дата периода
    '2024-09-01' AS end_date,    -- Конечная дата периода
    
    -- Определяем массив идентификаторов провайдеров и паттернов URL
    array(126, 231, 394, 412, 567, 742, 891, 1034, 1156, 1237, 1389, 1463, 1578, 2034, 2175, 2364, 2493, 2581, 2694, 2795, 2867, 3025, 3198, 3347, 
	3568, 3729, 3871, 4032, 4196, 4328, 4573, 4691, 4826, 4958, 5123, 5269, 5372, 5496, 5621, 5783, 5846, 5932, 6074, 6185, 6293,
	6472, 6598, 6734, 6890, 7031, 7145, 7294, 7436, 7548, 7683, 7815, 7984, 8127, 8295, 8472, 8593, 8734, 8921, 9084, 9246, 9378,
	9512, 9643, 9785, 9912, 10038, 10154, 10269, 10391, 10532, 10647, 10785, 10893, 11005, 11137, 11249, 11368, 11482, 11597, 11734,
	11849, 11963, 12078, 12195, 12304, 12439, 12572, 12689, 12834, 12946, 13057, 13198, 13362, 13487, 13654, 13791, 13924, 14038,
	14165, 14279, 14392, 14517, 14639, 14754, 14867, 15002, 15148, 15264, 15387, 15524, 15638, 15791, 15936, 16058, 16174, 16289, 
	33812, 33946, 34089, 34215, 34379, 34496, 34638, 34751, 34891, 35017, 35148, 35283, 35397, 35538, 35684, 35796, 35924, 36071, 36184, 
	36312, 36457, 36591, 36718, 36845, 36987, 37104, 37248, 37391, 37534, 37682, 37814, 37946, 38093, 38214, 38371, 38497, 38642, 38795, 
	38912, 39046, 39184, 39328, 39467, 39592, 39715, 39841, 39987, 40118, 40267, 40398, 40523, 40674, 40819, 40956, 41078, 41214, 41362, 
	41496, 41623, 41785, 41912, 42043, 42179, 42315, 42467, 42591, 42713, 42848, 42985, 43129, 43251, 43379, 43518, 43674, 43796, 43912, 
	44056, 44197, 44348, 44489, 44618, 44765, 44894, 45017, 45163, 45312, 45478, 45593, 45718, 45849, 45983, 46112, 46258, 46379, 46514, 
	46671, 46815, 46953) AS providers,  -- Массив идентификаторов провайдеров

    array(
        "product1/group1/item/0","product1/group1/item/1", "product1/group1/item/2", "product2/group2/item/0","product2/group2/item/1",
    "product2/group2/item/2", "product2/group2/item/3", "product2/group2/item/4", "product2/group2/item/5", "product3/group3/item/0",
    "product3/group3/item/1", "product3/group3/item/2", "product3/group3/item/3", "product3/group3/item/4", "product4/group4/item/main",
    "product4/group4/item/create", "product4/group4/item/edit", "product5/group5/item/0", "product5/group5/item/1", "product6/group6/item/0",
    "product6/group6/item/1", "product7/group7/item/0", "product7/group7/item/1", "product8/group8/item/0", "product8/group8/item/1",
    "product9/group9/item/0", "product9/group9/item/1", "product9/group9/item/2",  "product10/group10/item/0", "product11/group11/item/0",
    "product12/group12/item/0", "product12/group12/item/1", "product12/group12/item/2", "product12/group12/item/3", "product12/group12/item/4",
    "product12/group12/item/5", "product12/group12/item/6", "product12/group12/item/7", "product12/group12/item/8", "product12/group12/item/9",
    "product12/group12/item/10", "product12/group12/item/11", "product12/group12/item/12", "product12/group12/item/13") AS pattern,  -- Массив паттернов URL

    -- Агрегированные данные за указанный период
    aggregated_data AS (
        SELECT 
            toUInt64(providerId) AS providerId,  -- Преобразуем идентификатор провайдера в целое число
            pattern[multiMatchAnyIndex(pageUrl, pattern)] AS matched_pattern,  -- Находим первый совпавший паттерн в URL
            max(userDateTime) AS last_visit,  -- Находим последнее посещение
            argMax(userName, userDateTime) AS lastUserName  -- Находим имя пользователя, который последним посетил
        FROM snowplow.events_ru
        WHERE 
            userDateTime >= start_date AND userDateTime < addDays(toDate(end_date), 1)  -- Указываем период поиска записей
            AND eventName = 'page_view'  -- Фильтруем по событию "просмотр страницы"
            AND userIp NOT IN ('192.203.45.123', '10.1.2.3', '172.18.5.6', '192.168.2.4', '203.120.54.87', '88.44.33.99', '76.23.157.245')  -- Исключаем определенные IP-адреса
            AND joinGet(dbo.users_from_accounts_for_join, 'superuser', userName, toInt32(1)) = 0  -- Исключаем суперпользователей
            AND toUInt64(providerId) IN providers  -- Фильтруем по списку провайдеров
            AND multiMatchAny(pageUrl, pattern)  -- Фильтруем по паттернам URL
        GROUP BY providerId, matched_pattern  -- Группируем по идентификатору провайдера и совпавшему паттерну
    ),

    -- Расширяем t1 для работы с полным диапазоном дат
    t1 AS (
        SELECT
            url,   
            toDate(er_day) AS er_day,  -- Преобразуем er_day в дату
            toUInt64(providerId) AS providerId,  -- Преобразуем идентификатор провайдера в целое число
            0 AS is_viewed,  -- Флаг просмотра, по умолчанию 0
            '' AS userName,  -- Пустое имя пользователя
            toDateTime('0000-00-00 00:00:00') AS maxDate  -- Пустое значение даты
        FROM
        (
            SELECT
                arrayJoin(range(toUInt32(toDate(start_date) - toDate('1970-01-01')),    -- Генерируем диапазон дат от начала до конца периода
            toUInt32(toDate(end_date) - toDate('1970-01-01')) + 1)) AS er_day,        -- Конечная дата периода
                arrayJoin(pattern) AS url,  -- Разворачиваем массив паттернов в строки
                arrayJoin(providers) AS providerId  -- Разворачиваем массив провайдеров в строки
            ORDER BY er_day, providerId  -- Сортируем по дню и идентификатору провайдера
        )
    )

-- Основной запрос
SELECT
    Null as pageName,  -- Имя страницы, по умолчанию Null
    t1.url,  -- URL из временной таблицы t1
    Null as appName,  -- Имя приложения, по умолчанию Null
    year(t1.er_day) as current_year,  -- Извлекаем год из даты
    month(t1.er_day) as current_month,  -- Извлекаем месяц из даты
    day(t1.er_day) as current_day,  -- Извлекаем день из даты
    IF(aggregated_data.providerId IS NOT NULL AND t1.er_day <= aggregated_data.last_visit, 1, t1.is_viewed) AS is_viewed,  -- Проверяем, был ли просмотрен URL в указанный день
    IF(aggregated_data.providerId IS NOT NULL, aggregated_data.last_visit::date, Null) AS maxDate,  -- Указываем дату последнего посещения, если таковое было
    IF(aggregated_data.providerId IS NOT NULL, aggregated_data.lastUserName, t1.userName) AS userName,  -- Указываем имя пользователя, если таковое было
    concat(joinGet(dbo.users_from_accounts_for_join, 'first_name', userName, toInt32(1)), ' ', joinGet(dbo.users_from_accounts_for_join, 'last_name', userName, toInt32(1))) as firstAndLastNameOfUser,  -- Получаем полное имя пользователя
    t1.providerId as providerId,  -- Идентификатор провайдера
    dictGetString('dbo.dictionary_providerattribute_from_tlw', 'ProviderName', ifNull(toString(t1.providerId), Null)) as ProviderName,  -- Получаем имя провайдера
    dictGetString('dbo.dictionary_providerattribute_from_tlw', 'City', ifNull(toString(t1.providerId), Null)) as providerCity,  -- Получаем город провайдера
    dictGetString('dbo.dictionary_providerattribute_from_tlw', 'AccountManager', ifNull(toString(t1.providerId), Null)) as CSM  -- Получаем менеджера по аккаунту

FROM t1

LEFT JOIN aggregated_data 
    ON t1.providerId = aggregated_data.providerId 
    AND t1.url = aggregated_data.matched_pattern  -- Объединяем с агрегированными данными по идентификатору провайдера и совпавшему паттерну
    
