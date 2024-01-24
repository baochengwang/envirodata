
-- fuzzy string match
CREATE EXTENSION fuzzystrmatch;


-- #############################################################
--	FUNCTION TO CHECK THE CORRECTNESS OF PLZ AND ORT INPUT
-- #############################################################

-- _status code: 
-- 0 -- ORT and PLZ MATCH
-- 1 -- ORT-PLZ found but unmatch, use PLZ for searching
-- 2 -- PLZ not found, ORT found, use ORT for searching
-- 3 -- Both PLZ and ORT are not found. ERROR!

create or replace function check_plz_ort_correctness(ort text,plz integer, OUT _status integer)
language plpgsql
AS 
$$
DECLARE

--- USE `ILIKE` (case insensitive matching) for cityname fuzzy match instead of `~*`
--- IN THE LATTER CASE, 'Oberlindberg' matches 'Berlin' .
BEGIN

    IF EXISTS(select 1 from ort_plz where postonm ILIKE ort AND 
	postplz = plz limit 1) THEN

    _status := 0;

    ELSEIF EXISTS(SELECT 1 FROM ort_plz 
				  WHERE postplz = plz OR postonm ILIKE ort				  
				  limit 1)THEN
		IF EXISTS(SELECT 1 FROM ort_plz 
				  WHERE postplz = plz limit 1) THEN
			_status := 1;
		ELSE   
			_status :=2;
		END IF;
	
    ELSE
	_status := 3;
   
	END IF;

    RETURN;
END
$$



-- #############################################################
--	FUNCTION FOR FUZZYSTRMATCH
-- #############################################################

create or replace function address_match(i_str text,i_hnr integer, i_adz text,i_ort text,i_plz integer,
								 OUT street text, OUT house_number integer,OUT add_address text, 
								 OUT city text, OUT plz integer, 
								 OUT lat double precision, OUT lon double precision,
								 OUT logger integer)
RETURNS setof record
language plpgsql
AS
$$
declare
-- tolerance value for levenshtein string match  
lev_tol int := 3;

-- whether plz and ort(city name) matches? 
-- 0-perfect plz-ort match;     1- unmatched po, prioritize plz, 
-- 2-plz not found,prioritize ort; 3 - plz-ort are incorrect,please check the input.
-- 9: address is found, but PLZ might be misspelled.

po_status int ;
begin

select _status into po_status from check_plz_ort_correctness(i_ort,i_plz);


case 
	when po_status <=1 then  
		RETURN QUERY 
		SELECT str,hnr,adz,postonm,postplz,latitude,longitude, po_status as _log
			FROM hauskds WHERE 
			daitch_mokotoff(i_str) && daitch_mokotoff(str) AND
			levenshtein(lower(i_str),lower(str))<= lev_tol AND 
			hnr = i_hnr AND 
		 	adz ILIKE i_adz AND -- case insensitive matching 'A'='a' return True.  
			postplz = i_plz;
		IF NOT FOUND THEN 
			RETURN QUERY 
			SELECT str,hnr,adz,postonm,postplz,latitude,longitude, 9 as _log
				FROM hauskds WHERE 
				daitch_mokotoff(i_str) && daitch_mokotoff(str) AND
				levenshtein(lower(i_str),lower(str))<= lev_tol AND 
				hnr = i_hnr AND 
			 	adz ILIKE i_adz AND -- case insensitive matching 'A'='a' return True.  
				postonm ILIKE i_ort;
	when po_status = 2 then 
		RETURN QUERY 
		SELECT str,hnr,adz,postonm,postplz,latitude,longitude, po_status as _log
			FROM hauskds WHERE 
			daitch_mokotoff(i_str) && daitch_mokotoff(str) AND
			levenshtein(lower(i_str),lower(str))<= lev_tol AND 
			hnr = i_hnr AND 
			adz ILIKE i_adz AND
			postonm ILIKE i_ort;
	-- when po_status =3 then 
	-- 	raise notice 'PLZ AND ORT NOT FOUND!';
end case;

END
$$



