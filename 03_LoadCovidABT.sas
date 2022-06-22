/*Modify Load Code*/
proc fedsql sessref=casauto;
	create table COVID19.COVID19_ABT as
	select Combined_Key, Date, Country_Region as Country, Province_State as State, Admin2 as County, Population, PST045212 as CenPopulation, Confirmed, Deaths, ConfirmedDelta, DeathsDelta, 
	&bvals., maxtempF, mintempF, avgtempF, sunHour, uvIndex, DaytempF, DayprecipMM, Dayhumidity, Daypressure, 
	DaypressureInches, DayHeatIndexF, DayuvIndex, NighttempF, NightprecipMM, Nighthumidity, Nightpressure, 
	NightpressureInches, NightHeatIndexF, NightuvIndex, HSD310211 as avgHHSize, 
	INC910211 as PerCapitaInc, INC110211 as MedianInc, PVY020211 as PovPercent, LND110210 as LandAreaSQMI, POP060210 as CenPopDensity, (Population/LND110210) as PopDensity,
	Total_Active_MD, Total_Hospitals, Total_Hospital_Beds
	from COVID19.cov_wea_mob_pop b
	/*where Province_State  IN('New York', 'Pennsylvania', 'New Jersey', 'Connecticut', 'West Virginia', 'Maryland', 'Virginia', 'Delaware', 'Connecticut', 'Rhode Island', 'Massachusetts'
 , 'Vermont', 'New Hampshire'/*, 'Florida', 'Georgia', 'North Carolina', 'South Carolina', 'Maine')*/;
quit;

data COVID19.COVID19_ABT;
   set COVID19.COVID19_ABT;
   array change _numeric_;
        do over change;
            if change=. then change=0;
        end;
run ;


proc casutil incaslib='COVID19';
   promote casdata='COVID19_ABT' outcaslib='COVID19'; 
run; 

proc casutil incaslib='COVID19';
   save casdata='COVID19_ABT' outcaslib='COVID19' replace; 
run; 

data _NULL_;
   time_slept=sleep(5,1);
run;

/*cas casauto terminate;*/
