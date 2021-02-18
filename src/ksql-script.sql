CREATE SOURCE CONNECTOR postgres_component_price_connector WITH (
  'connector.class' = 'io.confluent.connect.jdbc.JdbcSourceConnector',
  'mode' = 'bulk',
  'topic.prefix' = 'postgres_',
  'connection.password' = '1234',
  'connection.user' = 'postgres',
  'poll.interval.ms' = '60000',
  'connection.url' = 'jdbc:postgresql://10.25.8.243:5432/',
  'table.whitelist' = 'component_price'
);

CREATE SOURCE CONNECTOR aoi_SAF831496TOP_V05_rest WITH (
  'connector.class' = 'io.confluent.connect.jdbc.JdbcSourceConnector',
  'mode' = 'timestamp',
  'timestamp.column.name' = 'insp_st_time',
  'topic.prefix' = 'aoi_SAF831496TOP_V05_',
  'connection.password' = '1234',
  'connection.user' = 'root',
  'poll.interval.ms' = '10000',
  'connection.url' = 'jdbc:mysql://10.25.14.54:3306/spc_a8hl16_038_2021020115590642',
  'table.whitelist' = 'insp_part, insp_array, insp_part_defect, insp_wind, insp_wind, insp_wind_defect'
);

CREATE SOURCE CONNECTOR aoi_SAF831496TOP_V05_insp_board WITH (
  'connector.class' = 'io.confluent.connect.jdbc.JdbcSourceConnector',
  'mode' = 'timestamp',
  'timestamp.column.name' = 'insp_st_time',
  'incrementing.column.name' = 'board_id',
  'validate.non.null' = 'false',
  'tasks.max' = '1',
  'query' = 'SELECT insp_st_time,  barcode, pass_code, defect_type, defect_code, board_id, line_no, lot_no, user_id, tack_time, module_good, module_ng, module_userok, module_skip, part_good, part_ng, part_userok, part_skip, slave_job FROM spc_a8hl16_038_2021020115590642.insp_board',
  'topic.prefix' = 'aoi_SAF831496TOP_V05_insp_board',
  'connection.password' = '1234',
  'connection.user' = 'root',
  'poll.interval.ms' = '10000',
  'connection.url' = 'jdbc:mysql://10.25.14.54:3306/spc_a8hl16_038_2021020115590642?zeroDateTimeBehavior=convertToNull'
);

CREATE SOURCE CONNECTOR spi_ART0822253E_V05_TOP_panel_insp_value WITH (
  'connector.class' = 'io.confluent.connect.jdbc.JdbcSourceConnector',
  'mode' = 'incrementing',
  'incrementing.column.name' = 'InspPanelIndex',
  'topic.prefix' = 'spi_ART0822253E_V05_TOP_',
  'connection.user' = 'root',
  'poll.interval.ms' = '10000',
  'connection.url' = 'jdbc:mysql://10.25.8.30:3306/ART0822253E_V05_TOP',
  'table.whitelist' = 'panel_insp_value'
);

CREATE SOURCE CONNECTOR spi_ART0822253E_V05_TOP_rest WITH (
  'connector.class' = 'io.confluent.connect.jdbc.JdbcSourceConnector',
  'mode' = 'incrementing',
  'incrementing.column.name' = 'InspPanelIndex',
  'topic.prefix' = 'spi_ART0822253E_V05_TOP_',
  'connection.user' = 'root',
  'poll.interval.ms' = '10000',
  'connection.url' = 'jdbc:mysql://10.25.8.30:3306/ART0822253E_V05_TOP',
  'table.whitelist' = 'pad_insp_result_1, board_insp_value, comp_insp_value_1, inspect_count'
);

CREATE SOURCE CONNECTOR jdbc_source_mssql_ipro WITH (
  'connector.class' = 'io.confluent.connect.jdbc.JdbcSourceConnector',
  'mode' = 'incrementing',
  'incrementing.column.name' = 'Prod_id',
  'topic.prefix' = 'ipro_',
  'connection.user' = 'iPro',
  'connection.password' = 'iProSupervision',
  'poll.interval.ms' = '10000',
  'connection.url' = 'jdbc:sqlserver://10.25.249.91:1433;databaseName=iPro',
  'table.whitelist' = 'Prod'
);

CREATE STREAM spi_ART0822253E_V05_TOP_board_insp_value_STREAM WITH (KAFKA_TOPIC='spi_ART0822253E_V05_TOP_board_insp_value',VALUE_FORMAT='AVRO');

CREATE STREAM spi_ART0822253E_V05_TOP_panel_insp_value_STREAM WITH (KAFKA_TOPIC='spi_ART0822253E_V05_TOP_panel_insp_value',VALUE_FORMAT='AVRO');

SELECT B.INSPPANELINDEX AS INSPPANELINDEX, B.BOARDID AS BOARDID, B.HEIGHT AS HEIGHT, B.AREA AS AREA, B.VOLUME AS VOLUME, B.OFFSETX AS OFFSETX, B.OFFSETY AS OFFSETY, B.HEIGHTPER AS HEIGHTPER, B.AREAPER AS AREAPER, B.VOLUMEPER AS VOLUMEPER, B.WARPAGE AS WARPAGE, P.MODELCODE AS MODELCODE, P.BARCODEDATA AS BARCODEDATA FROM SPI_ART0822253E_V05_TOP_BOARD_INSP_VALUE_STREAM B INNER JOIN SPI_ART0822253E_V05_TOP_PANEL_INSP_VALUE_STREAM P WITHIN 60 SECONDS ON B.INSPPANELINDEX = P.INSPPANELINDEX EMIT CHANGES LIMIT 10;

CREATE STREAM oib_component_details_STREAM (
    COMPONENTABSENCEAFTERPICK INTEGER, 
    COMPONENTABSENCEBEFOREPLACE INTEGER, 
    COMPONENTMATERIALDEFECT INTEGER, 
    TREATMENTERROR INTEGER, 
    DROPPEDERROR INTEGER, 
    COMPONENTPRESENCEAFTERPLACE INTEGER, 
    IDENTERROR INTEGER, 
    PICKUPRETRIES INTEGER, 
    COMPONENTABSENCEAFTERPICK_INT INTEGER, 
    COMPONENTABSENCEBEFOREPLACE_INT INTEGER, 
    COMPONENTMATERIALDEFECT_INT INTEGER, 
    TREATMENTERROR_INT INTEGER, 
    DROPPEDERROR_INT INTEGER, 
    COMPONENTPRESENCEAFTERPLACE_INT INTEGER, 
    IDENTERROR_INT INTEGER, 
    PICKUPRETRIES_INT INTEGER, 
    MESSAGEOID INTEGER, 
    TABLELOCATION INTEGER, 
    TRACK INTEGER, 
    TOWER INTEGER,
    LEVEL INTEGER, 
    DIVISION INTEGER, 
    FEEDERTYPE INTEGER, 
    COMPONENTNAME VARCHAR, 
    COMPONENTFULLPATH VARCHAR, 
    COMPONENTSHAPENAME VARCHAR, 
    COMPONENTSHAPEFULLPATH VARCHAR, 
    FEEDERLONGTYPE INTEGER, 
    FEEDERTYPENAME VARCHAR, 
    TABLETYPENAME VARCHAR, 
    USED INTEGER, 
    TABLETYPE INTEGER, 
    LINENAME VARCHAR, 
    LINEFULLPATH VARCHAR, 
    NAME VARCHAR, 
    FULLPATH VARCHAR, 
    PROCESSINGAREACOUNT INTEGER, 
    TEXT VARCHAR, 
    COMPUTERADDRESS VARCHAR, 
    MACHINEID VARCHAR, 
    SOFTWAREVERSION VARCHAR, 
    TYPENAME VARCHAR, 
    MACHINETYPE INTEGER, 
    STATIONORDER INTEGER, 
    ISA95PATH VARCHAR, 
    ACCESSTOTAL_INT INTEGER, 
    REJECTIDENT_INT INTEGER, 
    REJECTVACUUM_INT INTEGER, 
    PLACEDCOMPONENTS_INT INTEGER, 
    STATIONTIME VARCHAR, 
    BOARDNAME VARCHAR, 
    BARCODE VARCHAR) 
    WITH (KAFKA_TOPIC='oib_component_details', 
          VALUE_FORMAT='JSON',
          PARTITIONS = 1);

SET 'auto.offset.reset' = 'earliest';

CREATE STREAM postgres_component_price_STREAM WITH (KAFKA_TOPIC='postgres_component_price',VALUE_FORMAT='AVRO');

CREATE STREAM IND_COMP_PRICE_STREAM as select ARTICLE, GET_PRICE(CAST(REPLACE(REPLACE(PRIX_STANDARD, ',', '.'), ' ', '') AS DOUBLE)) PRICE from POSTGRES_COMPONENT_PRICE_STREAM emit changes;

CREATE STREAM IND_COMP_PRICE_WITH_ID as select GET_NAMEID(ARTICLE) as COMPONENT_ID, ARTICLE, PRICE from IND_COMP_PRICE_STREAM emit changes;

CREATE TABLE COMP_PRICE_TABLE as select COMPONENT_ID, SUM(PRICE) as PRICE from IND_COMP_PRICE_WITH_ID GROUP BY COMPONENT_ID emit changes;

CREATE STREAM COMPONENT_DETAILS_WITH_ID as SELECT GET_NAMEID(c.ComponentName) AS component_id, c.ComponentName, c.ComponentShapeName, c.ComponentAbsenceAfterPick_Int, c.ComponentAbsenceBeforePlace_Int, c.ComponentMaterialDefect_Int, c.TreatmentError_Int, c.DroppedError_Int, c.IdentError_Int, c.LineName, c.Name as MachineName, c.AccessTotal_Int, c.RejectIdent_Int, c.RejectVacuum_Int, c.PlacedComponents_Int, c.StationTime, c.BoardName, c.Barcode FROM OIB_COMPONENT_DETAILS_STREAM c EMIT CHANGES;

CREATE STREAM COMPONENT_DETAILS_PRICE_LIGNE3 WITH (KAFKA_TOPIC='COMPONENT_DETAILS_PRICE_LIGNE3', PARTITIONS=1, REPLICAS=1) AS SELECT c.component_id as component_id, c.ComponentName as ComponentName, p.PRICE, c.ComponentShapeName, c.ComponentAbsenceAfterPick_Int, c.ComponentAbsenceBeforePlace_Int, c.ComponentMaterialDefect_Int, c.TreatmentError_Int, c.DroppedError_Int, c.IdentError_Int, c.LineName, c.MachineName, c.AccessTotal_Int, c.RejectIdent_Int, c.RejectVacuum_Int, c.PlacedComponents_Int, c.StationTime, c.BoardName, c.Barcode FROM COMPONENT_DETAILS_WITH_ID c LEFT JOIN COMP_PRICE_TABLE p ON c.component_id = p.component_id where c.LineName = 'Ligne3' EMIT CHANGES;

CREATE STREAM REJECT_RATE_LIGNE3 WITH (KAFKA_TOPIC='REJECT_RATE_LIGNE3', PARTITIONS=1, REPLICAS=1) AS SELECT COMPONENT_ID, COMPONENTNAME, PRICE, COMPONENTSHAPENAME, LINENAME, MACHINENAME, STATIONTIME, BOARDNAME, BARCODE, REJECT_RATE(COMPONENTABSENCEAFTERPICK_INT, COMPONENTABSENCEBEFOREPLACE_INT, COMPONENTMATERIALDEFECT_INT, TREATMENTERROR_INT, DROPPEDERROR_INT, IDENTERROR_INT, 0, REJECTIDENT_INT, REJECTVACUUM_INT, ACCESSTOTAL_INT) REJECT_RATE, REJECT_RATE(PLACEDCOMPONENTS_INT, ACCESSTOTAL_INT, PRICE) REJECT_VALUE, ACCESSTOTAL_INT, PLACEDCOMPONENTS_INT FROM COMPONENT_DETAILS_PRICE_LIGNE3 EMIT CHANGES;

CREATE STREAM REJECT_RATE_LIGNE3 WITH (KAFKA_TOPIC='REJECT_RATE_LIGNE3', PARTITIONS=1, REPLICAS=1) AS SELECT COMPONENT_ID, COMPONENTNAME, PRICE, COMPONENTSHAPENAME, LINENAME, MACHINENAME, STATIONTIME, BOARDNAME, BARCODE, REJECT_RATE(PLACEDCOMPONENTS_INT, ACCESSTOTAL_INT, PRICE) REJECT_VALUE, ACCESSTOTAL_INT, PLACEDCOMPONENTS_INT FROM COMPONENT_DETAILS_PRICE_LIGNE3 EMIT CHANGES;

CREATE SINK CONNECTOR REJECT_RATE_LIGNE3_ELASTIC WITH (
  'connector.class' = 'io.confluent.connect.elasticsearch.ElasticsearchSinkConnector',
  'type.name' = '_doc',
  'topics' = 'REJECT_RATE_LIGNE3',
  'value.converter.schemas.enable' = 'false',
  'value.converter' = 'org.apache.kafka.connect.json.JsonConverter',
  'connection.url' = 'http://10.25.249.5:9200',
  'key.ignore' = 'true',
  'key.converter' = 'org.apache.kafka.connect.storage.StringConverter',
  'schema.ignore' = 'true'
);

CREATE SINK CONNECTOR COMPONENT_DETAILS_PRICE_LIGNE3_ELASTIC WITH (
  'connector.class' = 'io.confluent.connect.elasticsearch.ElasticsearchSinkConnector',
  'type.name' = '_doc',
  'topics' = 'COMPONENT_DETAILS_PRICE_LIGNE3',
  'value.converter.schemas.enable' = 'false',
  'value.converter' = 'org.apache.kafka.connect.json.JsonConverter',
  'connection.url' = 'http://10.25.249.5:9200',
  'key.ignore' = 'true',
  'key.converter' = 'org.apache.kafka.connect.storage.StringConverter',
  'schema.ignore' = 'true'
);








