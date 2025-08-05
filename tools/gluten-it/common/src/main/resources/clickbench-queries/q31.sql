SELECT SearchEngineID, SUM(IsRefresh) FROM hits WHERE SearchPhrase <> '' AND (SearchEngineID in (1, 2, 4)) AND ClientIP = -807147100 GROUP BY SearchEngineID
