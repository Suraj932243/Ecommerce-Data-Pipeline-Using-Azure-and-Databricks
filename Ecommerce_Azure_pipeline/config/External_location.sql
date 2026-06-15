CREATE EXTERNAL LOCATION bronze_location
URL 'abfss://bronze@sdpprostorage.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sdp_storage_credential);

SHOW EXTERNAL LOCATIONS;

CREATE EXTERNAL LOCATION silver_location
URL 'abfss://silver@sdpprostorage.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sdp_storage_credential);

CREATE EXTERNAL LOCATION gold_location
URL 'abfss://gold@sdpprostorage.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sdp_storage_credential);