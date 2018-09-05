CREATE VIEW ARTIST_VIEW AS
					SELECT DISTINCT t.ARTIST_ID as artist_id_comb
					FROM TRACK t
					WHERE t.ARTIST_ID IS NOT NULL
					
					UNION ALL
					
					SELECT DISTINCT t.ARTIST_ID2 as artist_id_comb
					FROM TRACK t
					WHERE t.ARTIST_ID2 IS NOT NULL
					
					UNION ALL
					
					SELECT DISTINCT t.ARTIST_ID3 as artist_id_comb
					FROM TRACK t
					WHERE t.ARTIST_ID3 IS NOT NULL
					
					UNION ALL
					
					SELECT DISTINCT t.ARTIST_ID4 as artist_id_comb
					FROM TRACK t
					WHERE t.ARTIST_ID4 IS NOT NULL