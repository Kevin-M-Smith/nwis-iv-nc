ALTER TABLE meta_station ADD COLUMN huc_l1 text; 
ALTER TABLE meta_station ADD COLUMN huc_l2 text; 
ALTER TABLE meta_station ADD COLUMN huc_l3 text; 

CREATE TABLE hucs (huc_l1 text, huc_l2 text, huc_l3 text, huc_l4 text);

INSERT INTO hucs (huc_l1, huc_l2, huc_l3, huc_l4)
SELECT 
	substring(huc_cd from 1 for 2),
	substring(huc_cd from 1 for 4),
	substring(huc_cd from 1 for 6),
	substring(huc_cd from 1 for 8)
FROM meta_station;

UPDATE meta_station
set huc_l1 = hucs.huc_l1,
	huc_l2 = hucs.huc_l2,
	huc_l3 = hucs.huc_l3
from hucs
where
	meta_station.huc_cd = hucs.huc_l4;

DROP TABLE hucs;