CREATE TABLE rtb_brands (
	brand VARCHAR(50),
	pc VARCHAR(50),
	date_month VARCHAR(50),    
	revenue INTEGER CHECK (revenue > 0)  /* na tym etapie mozna rozwazyc dodanie Constraint, ze revenue > 0 */

/*IMPORT pliku excel - Brands_revenue_task z danymi z interfejsu PostgreSQL   */

/*QUERIES   */	

WITH option_1 AS (
/*miesiąc pierwszego wystąpienia przychodu na danym rynku w całej historii współpracy z firmą X  */
	SELECT brand, pc,
		TO_DATE(translate(date_month,'-',''), 'YYYYMM') as month_0, /*usuwam - i konwertuje na date format  */ 
		min(date_month) OVER(partition by brand, pc) as fm_1,   /*data wg 1 definicji  */
		revenue
	
FROM rtb_brands
GROUP BY brand,pc,date_month, revenue
ORDER BY brand, pc, date_month),

option_2 AS  /* Współpraca rozpoczyna się od nowa, jeśli 
brand nie miał przychodu przez co najmniej 12 ostatnich miesięcy. */
(
SELECT *,
LAG(month_0) OVER(Partition by brand, pc) as prev_month,     /* nowa kolumna z wartoscia z poprzedniego miesiaca     */ 
month_0 - LAG(month_0) OVER(Partition by brand, pc) as day_change,    /* roznica dni miedzy obecna, a poprzednia data      */
CASE 
	WHEN (month_0 - LAG(month_0) OVER(Partition by brand, pc) > 365) and revenue > 0  /* gdy roznica miedzy obecna data, a poprzednia jest wieksza niz 365 dni (12m) i przychod jest dodatni, 
	w nowa kolumne zaciagam obecny rok i miesiac, wartosc musi byc jako string!, jesli NIE to nadpisuje wartosc z opcji_1 (fm_1) zostawiajac obecna date, gdyz klient NIE jest nowy wg definicji 2 */
	THEN CAST(month_0 as VARCHAR(7))
	ELSE fm_1
	END as fm_2
	
FROM option_1
),

/* sprawdzam (1) kiedy brand + rynek jest liczony drugi raz jako nowy klient  */
check_events AS (
	SELECT brand,pc,month_0,fm_1,fm_2,revenue,
CASE
	WHEN fm_1 <> fm_2 THEN 1
	WHEN fm_1 = fm_2 THEN 0
	ELSE NULL
	END AS checking
FROM option_2
GROUP BY brand,pc,month_0,fm_1,fm_2,revenue
ORDER BY checking DESC )

/* OPCJA 3 Współpraca rozpoczyna się od nowa, 
jeśli brand nie nie miał przychodu w poprzednim kwartale i ma przychód w obecnym kwartale     */

/* Przygotowanie YEAR + QUARTER - opcja 3   */
SELECT brand, pc, 
CAST(EXTRACT (YEAR FROM month_0) AS TEXT) || '-' ||
CAST(EXTRACT (QUARTER FROM month_0) AS TEXT) || 'Q' as Q_YEAR,
/* sum(revenue) as quarter_revenue */
month_0, fm_1, fm_2, revenue

FROM check_events
GROUP BY brand, pc, month_0, q_year, fm_1, fm_2, revenue
ORDER BY brand, pc,month_0, q_year

/* teraz trzeba by zrobic, ze gdy w posortowanym q_year gdy NIE ma w kolejnosci dla tego samego BRANDU i PC
sytuacji 2013-Q1, 2013-Q2, 2013-Q3 LUb gdy NIE ma ze te same okresy sa pod soba 2018-4Q, 2018-4Q itd to wtedy znaczy ze nie ma quarter_revenue
brakuje w danym kwartale REVENUE wiec nalezaloby wziac ta date z tego okresu (month_0) i wpisac do nowej kolumny fm_3... */
