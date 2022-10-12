CREATE OR REPLACE FUNCTION PARSE_CSV(CSV STRING, DELIMITER STRING, QUOTECHAR STRING)
RETURNS TABLE (V VARIANT)
LANGUAGE PYTHON
RUNTIME_VERSION=3.8
HANDLER='CsvParser'
AS $$
import csv

class CsvParser:
    def __init__(self):
        # Allow fields up to the VARCHAR size limit
        csv.field_size_limit(16777216)
        self._isFirstRow = True
        self._headers = []

    def process(self, CSV, DELIMITER, QUOTECHAR):
        # If the first row in a partition, store the headers
        if self._isFirstRow:
            self._isFirstRow = False
            # csv.reader to split up the headers
            reader = csv.reader([CSV], delimiter=DELIMITER, quotechar=QUOTECHAR)
            # Convert field names to upper case for consistency
            self._headers = list(map(lambda h: h.upper(), list(reader)[0]))
        else:
            # A DictReader allows us to provide the headers as a parameter
            reader = csv.DictReader(
                [CSV],
                fieldnames=self._headers,
                delimiter=DELIMITER,
                quotechar=QUOTECHAR,
            )
            for row in reader:
                # CSV are often sparse because each record has every field
                # Remove empty values to improve performance
                res = { k:v for (k,v) in row.items() if v }
                yield (res,)
$$;

-- Create a file format that won't split up records by a delimiter
CREATE FILE FORMAT IF NOT EXISTS TEXT_FORMAT 
TYPE = 'CSV' 
FIELD_DELIMITER = NONE
SKIP_BLANK_LINES = TRUE
ESCAPE_UNENCLOSED_FIELD = NONE;

-- Query specifying delimiter and enclosure
SELECT 
  CSV_PARSER.V, 
  STG.METADATA$FILENAME as FILENAME, 
  STG.METADATA$FILE_ROW_NUMBER as FILE_ROW_NUMBER,
  STG.$1 AS ORIGINAL_TEXT
-- Query the stage for one file or use a pattern for multiple
FROM @TEST_STAGE (FILE_FORMAT => TEXT_FORMAT, PATTERN=>'.*.csv.gz') STG
-- Lateral join to call our UDTF
JOIN LATERAL PARSE_CSV(STG.$1, ',', '"') 
  -- Partition by file to support multiple files at once
  OVER (PARTITION BY METADATA$FILENAME 
  -- Order by row number to ensure headers are first in each window
  ORDER BY METADATA$FILE_ROW_NUMBER) AS CSV_PARSER;

-- Examples of pulling out fields later
WITH VARIANT_DATA AS (
SELECT V
FROM @TEST_STAGE (FILE_FORMAT => TEXT_FORMAT, PATTERN=>'.*.csv.gz') STG
JOIN LATERAL PARSE_CSV(STG.$1, ',', '"') OVER 
  (PARTITION BY METADATA$FILENAME ORDER BY METADATA$FILE_ROW_NUMBER)
)
SELECT 
  V:ORDER_DAY_DT::DATE AS BILL_DAY_DT,
  V:ORDER_DTIME1_DB_TZ::TIMESTAMP AS ORDER_DTIME1_DB_TZ,
  V:BILL_MTH_KEY::INT AS BILL_MTH_KEY,
  V:COST_FIXED::NUMBER(18,2) AS COST_FIXED,
  V:CURRENCY::VARCHAR(3) AS CURRENCY
FROM VARIANT_DATA;