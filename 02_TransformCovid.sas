/*Data Step for Reading Weather Data*/
/*Adding a Comment in Github*/
data COVID19.cov_wea;
  keep Combined_Key Date LastDate Country_Region Province_State Admin2 Population Confirmed Deaths maxtempF mintempF avgtempF sunHour uvIndex 
  DaytempF DayprecipMM Dayhumidity Daypressure DaypressureInches DayHeatIndexF DayuvIndex NighttempF NightprecipMM Nighthumidity 
  Nightpressure NightpressureInches NightHeatIndexF NightuvIndex;
  infile cov_wea dsd truncover  firstobs=2;                            
  input UID :$10. iso2 :$2. iso3 :$3. code3 :$5. FIPS :$6.  Admin2 :$40. Province_State :$40. Country_Region :$40. 
  Lat :2.8 Long_ :2.8 Combined_Key :$80. Date :yymmdd10. Confirmed :8. Population :8. Deaths :8. Daily_Confirmed :8. Daily_Deaths :8. 
  Daily_Recovered :8. Daily_Active :8. maxtempF :8.2 mintempF :8.2 avgtempF :8.2 totalSnow_cm :8.2 sunHour :8.2 uvIndex :8.2 DaytempF :8.2 
  DaywindspeedMiles :8.2 DayweatherCode :8.2 DayprecipMM :8.2 Dayhumidity :8.2 Daypressure :8.2 DaypressureInches :8.2 DayHeatIndexF :8.2 
  DayDewPointF :8.2 DayWindChillF :8.2 DayFeelsLikeF :8.2 DayuvIndex :8.2 NighttempF :8.2 NightwindspeedMiles :8.2 NightweatherCode :8.2 
  NightprecipMM :8.2 Nighthumidity :8.2 Nightpressure :8.2 NightpressureInches :8.2 NightHeatIndexF :8.2 NightDewPointF :8.2 NightWindChillF :8.2 
  NightFeelsLikeF :8.2 NightuvIndex :8.2;
  format LastDate Date yymmdd10.;
  LastDate = intnx('day',date,-1);
  Combined_Key = upcase(tranwrd(Combined_Key, ", ", ","));
  Combined_Key = upcase(tranwrd(Combined_Key, " ,", ","));
  if Date >= MDY(3,1,2020) AND NOT(Combined_Key IN('QUEENS,NEW YORK,US', 'KINGS,NEW YORK,US', 'BRONX,NEW YORK,US', 'RICHMOND,NEW YORK,US'));
run;

/*Modify Renames*/
data COVID19.cov_wea;
	set COVID19.cov_wea;
	if upcase(Combined_Key) = 'NEW YORK CITY,NEW YORK,US' then 
    do;
	    do word='New York','Richmond', 'Kings', 'Bronx', 'Queens';
	      Combined_Key = upcase(strip(word)) || ',NEW YORK,US';
		  Admin2 = strip(word);
		  output;
	    end;
    end;
	else output;
run;


data COVID19.cov_mob;
  keep Combined_Key state county category Date value;
  infile cov_mob dsd truncover  firstobs=2;                            
  input state :$40. county :$40. category :$30. page :8. change :8.2 changecalc :8.10 Date :yymmdd10. value :8.4 Combined_Key :$80.;
  format Date yymmdd10.;
  Combined_Key = upcase(tranwrd(Combined_Key, ", ", ","));
run;

data COVID19.cov_pop;
  keep State County Combined_Key PST045212 HSD310211 INC910211 INC110211 PVY020211 LND110210 POP060210; 
  infile cov_pop dsd truncover  firstobs=2;                            
  input fips :$6. PST045212 :8.2 PST040210 :8.2 PST120212 :8.2 POP010210 :8.2 AGE135212 :8.2 AGE295212 :8.2 AGE775212 :8.2 SEX255212 :8.2 
  RHI125212 :8.2 RHI225212 :8.2 RHI325212 :8.2 RHI425212 :8.2 RHI525212 :8.2 RHI625212 :8.2 RHI725212 :8.2 RHI825212 :8.2 POP715211 :8.2 
  POP645211 :8.2 POP815211 :8.2 EDU635211 :8.2 EDU685211 :8.2 VET605211 :8.2 LFE305211 :8.2 HSG010211 :8.2 HSG445211 :8.2 HSG096211 :8.2 
  HSG495211 :8.2 HSD410211 :8.2 HSD310211 :8.2 INC910211 :8.2 INC110211 :8.2 PVY020211 :8.2 BZA010211 :8.2 BZA110211 :8.2 BZA115211 :8.2 
  NES010211 :8.2 SBO001207 :8.2 SBO315207 :8.2 SBO115207 :8.2 SBO215207 :8.2 SBO515207 :8.2 SBO415207 :8.2 SBO015207 :8.2 MAN450207 :8.2 
  WTN220207 :8.2 RTN130207 :8.2 RTN131207 :8.2 AFN120207 :8.2 BPS030212 :8.2 LND110210 :8.2 POP060210 :8.2 County :$40. State :$40. Combined_Key :$80.;
  Combined_Key = upcase(tranwrd(Combined_Key, ", ", ","));
run;

/*Calculate Deltas*/
proc fedsql sessref=casauto;
	create table COVID19.cov_wea_del as
	select a.*, (a.Confirmed - b.Confirmed) as ConfirmedDelta, (a.Deaths - b.Deaths) as DeathsDelta
	from COVID19.cov_wea a left join COVID19.cov_wea b ON(a.Combined_Key = b.Combined_Key and a.LastDate = b.Date);
quit;

data  COVID19.cov_mob_cal(drop=caregory_tr);
	set COVID19.cov_mob;
	caregory_tr = tranwrd(category, "/", "_");
	category = caregory_tr;
run;

proc sort data=COVID19.cov_mob_cal out=COVID19.cov_mob_cal_dedup NODUPKEY;
   by Combined_Key state county Date category;
run;

proc transpose data=COVID19.cov_mob_cal_dedup
	out=COVID19.cov_mob_trans
	name=value;
	by state county Date Combined_Key;
	id category;
run;

data  COVID19.cov_mob_trans(drop=state county value);
	set COVID19.cov_mob_trans;
run;

proc sql noprint;
     select strip(name) into : bvals separated by ', b.' from dictionary.columns
         where upcase(libname)="SESSLIB" and upcase(memname)="COV_MOB_TRANS" and upcase(name) ^= 'COMBINED_KEY' and  upcase(name) ^= 'DATE';
quit;

%put The vars are: &bvals.;

data HRSA.Toms_file;
	set HRSA.Toms_file;
	Combined_Key = upcase(tranwrd(Combined_Key, ", ", ","));
run;

proc fedsql sessref=casauto;
	create table COVID19.cov_wea_mob_pop as
	select a.Combined_Key, a.Date, a.Country_Region, a.Province_State, a.Admin2, a.Population, a.Confirmed, a.Deaths, a.ConfirmedDelta, a.DeathsDelta, 
	a.maxtempF, a.mintempF, a.avgtempF, a.sunHour, a.uvIndex, a.DaytempF, a.DayprecipMM, a.Dayhumidity, a.Daypressure, 
	a.DaypressureInches, a.DayHeatIndexF, a.DayuvIndex, a.NighttempF, a.NightprecipMM, a.Nighthumidity, a.Nightpressure, 
	a.NightpressureInches, a.NightHeatIndexF, a.NightuvIndex, b.&bvals., c.PST045212, c.HSD310211, c.INC910211, c.INC110211, c.PVY020211, c.LND110210, c.POP060210,
	d.Total_Active_MD, d.Total_Hospitals, d.Total_Hospital_Beds
	from COVID19.cov_wea_del a 
		left join COVID19.cov_mob_trans b ON(a.Combined_Key = b.Combined_Key and a.Date = b.Date) 
		left join COVID19.cov_pop c ON(a.Combined_Key = c.Combined_Key) 
		left join HRSA.Toms_file d ON(a.Combined_Key = d.Combined_Key);
quit;

data COVID19.cov_wea_mob_pop;
   set COVID19.cov_wea_mob_pop;
   array change _numeric_;
        do over change;
            if change=0 then change=.;
        end;
run ;


data _NULL_;
   time_slept=sleep(5,1);
run;

/*cas casauto terminate;*/
