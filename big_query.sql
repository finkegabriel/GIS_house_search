SELECT * FROM `orobytes.az_homes.az_homes_geometry` LIMIT 1000
UPDATE `orobytes.az_homes.az_homes_geom`
SET geometry = ST_GEOGFROMGEOJSON(geometry_geojson);
CREATE TABLE `orobytes.az_homes.az_homes_geometry` AS
SELECT
  *,
  ST_GEOGFROMGEOJSON(geometry_geojson) AS geom
FROM
  `orobytes.az_homes.az_homes_geoms`;
SELECT *
SELECT
  *,
  SAFE.ST_GEOGFROMGEOJSON(geometry_geojson) AS geom
FROM
  `orobytes.az_homes_geometry`
WHERE SAFE.ST_GEOGFROMGEOJSON(geometry_geojson) IS NOT NULL;
create table az_homes_geometry as SELECT
  *,
  SAFE.ST_GEOGFROMGEOJSON(geometry_geojson) AS geom
FROM
  `orobytes.az_homes_geometry`
WHERE SAFE.ST_GEOGFROMGEOJSON(geometry_geojson) IS NOT NULL;


CREATE TABLE `orobytes.az_homes.az_homes_geometry` AS
SELECT
  *,
  SAFE.ST_GEOGFROMGEOJSON(geometry_geojson) AS geom
FROM
  `orobytes.az_homes.az_homes_geoms`
WHERE
  SAFE.ST_GEOGFROMGEOJSON(geometry_geojson) IS NOT NULL;

ALTER TABLE `orobytes.az_homes.az_homes_geometry` ADD COLUMN good_geom GEOGRAPHY;
UPDATE `orobytes.az_homes.az_homes_geometry`
SET geometry = ST_GEOGFROMGEOJSON(geo);



