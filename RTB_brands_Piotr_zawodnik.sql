

CREATE TABLE rtb_brands (
	brand VARCHAR(50),
	pc VARCHAR(50),
	date_month VARCHAR(50),    
	revenue INTEGER /* na tym etapie mozna rozwazyc dodanie Constraint CHECK (revenue > 0) */

/*IMPORT pliku excel - Brands_revenue_task z danymi z interfejsu PostgreSQL 14.0   */

/*QUERIES   */	


WITH option_1 AS (

	SELECT brand, pc,
		TO_DATE(translate(date_month,'-',''), 'YYYYMM') as month_0, /*usuwam - i konwertuje na date format  */ 
		min(date_month) OVER(partition by brand, pc) as fm_1,   /*data wg 1 definicji  */
		revenue
	
FROM rtb_brands
GROUP BY brand,pc,date_month, revenue
ORDER BY brand, pc, date_month),

option_2_days AS
( 
SELECT *,
LAG(month_0) OVER(Partition by brand, pc) as prev_month,     /* nowa kolumna z wartoscia z ostatniego aktywnego miesiaca     */ 
month_0 - LAG(month_0) OVER(Partition by brand, pc) as day_change     /* roznica dni miedzy obecnym Month a ostatnim aktywnym miesiacem      */
FROM option_1
WHERE revenue > 0 ),    /*rozwazamy przypadki gdy revenue jest wieksze od 0      */

option_2_months AS       /*licze roznice miesiecy miedzy obecnym a ostatnim aktywnym miesiacem (brak funkcji Datediff() w postgresql)    */
( 
SELECT *,
		(DATE_PART('year', month_0) - DATE_PART('year', prev_month)) * 12 +
        (DATE_PART('month', month_0) - DATE_PART('month', prev_month)) as month_difference
FROM option_2_days
),

option_2 AS
(
SELECT *,
CASE    /* jezeli roznica miesiecy miedzy obecna data a ostatnia jest wieksza od 12M to wpisz nowa date poczatkowa
             jesli NIE to wstaw ta kontyunacja biznesu z data poprzednia fm_1*/
	WHEN month_difference > 12 THEN CAST(month_0 as VARCHAR(7)) 
	ELSE fm_1                                /* Uwaga blad! - w przypadku gdy month >12M a potem wzrosty od 1-12m wpisuje wtedy blednie date poczatkowa zamiast nowa poprzednia      */
	END as fm_2
FROM option_2_months ),

/* sprawdzam (1) kiedy brand + rynek jest liczony drugi raz jako nowy klient z nowa data  */
check_events AS (
	SELECT brand,pc,month_0,fm_1,fm_2,revenue,
CASE
	WHEN fm_1 <> fm_2 THEN 1
	WHEN fm_1 = fm_2 THEN 0            /* dodatkowe */
	ELSE NULL
	END AS checking
FROM option_2
GROUP BY brand,pc,month_0,fm_1,fm_2,revenue
ORDER BY checking DESC ),

/* OPCJA 3 - Przygotowanie YEAR + QUARTER   */
option_3_dates AS (
SELECT *,
CAST(EXTRACT (YEAR FROM month_0) AS TEXT) || '-' ||
CAST(EXTRACT (QUARTER FROM month_0) AS TEXT) || 'Q' as quarter,

EXTRACT(YEAR FROM month_0) AS years,
EXTRACT(QUARTER FROM month_0) AS q
FROM option_2
ORDER BY brand, pc,month_0, quarter ),

option_3_LY AS (
SELECT *,
LAG(years) OVER(Partition by brand, pc) as prev_year,
LAG(q) OVER(Partition by brand, pc) as prev_q
FROM option_3_dates),

option_3 AS (
SELECT *,
	CASE
		WHEN prev_year IS NULL THEN fm_1    /*jezeli prev_year jest NULL znaczy ze klient zaczal wtedy pierwsza aktywnosc - wiec data poczatkowa fm_1     */

	              /*sa 3 mozliwosci gdy powinna zostac stara data: (1) daty sa takie same, (2) rok taki sam, ale wzrost kwartalu tylko o 1, lub (3)
	                  sytuacja 2018-Q4 : 2019-Q1 czyli przejscie roku o 1 i kwartalu o 3 */

		WHEN (years = prev_year AND q = prev_q) OR (years = prev_year AND q - prev_q = 1) OR (years - prev_year = 1 AND prev_q - q =3) THEN fm_1

	             /*  w innych sytuacjach klient mial brak aktywnosci >1 kwartal wiec NOWA data dzialanosci month_0     */

		ELSE CAST(month_0 as VARCHAR(7))
		END AS fm_3                                     
		
FROM option_3_LY )

/* ZADANIA z Polecenia 2:      */

SELECT brand,pc, count(distinct(fm_3,brand,pc)) as fm_3_count,
sum(count(distinct(fm_3,brand,pc))) OVER () as number_of_clients
FROM option_3
WHERE years = '2018'
GROUP BY brand,pc
ORDER BY fm_3_count DESC