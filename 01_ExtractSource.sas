/*Uncomment CAS Session start and terminate for running in SAS Studio. Job Execution Handles for Batch.*/
cas casauto sessopts=(caslib='casuser');

caslib _all_ assign;
libname sesslib cas caslib=COVID19;

/*Updating the Files Being Read Again Via Github*/
filename cov_wea "/opt/sas/data/COVID19-Targets/covid_weather.csv";
filename cov_mob "/opt/sas/data/COVID19-Targets/covid_mobility.csv";
filename cov_pop "/opt/sas/data/COVID19-Targets/county_population.csv";


proc cas;

function doesTableExist(casLib, casTable);
  table.tableExists result=r status=rc / caslib=casLib table=casTable;
  tableExists = dictionary(r, "exists");
  return tableExists;
end func;
run;

function dropTableIfExists(casLib,casTable);
  tableExists = doesTableExist(casLib, casTable);
  if tableExists != 0 then do;
    print "Dropping table: "||casLib||"."||casTable;
    table.dropTable result=r status=rc/ caslib=casLib table=casTable quiet=0;
	table.deleteSource / caslib=casLib source=casTable||".sashdat" quiet=TRUE;
    if rc.statusCode != 0 then do;
      exit();
    end;
  end;
end func;
run;

function loadTableNotExists(casLib,casTable);
  tableExists = doesTableExist(casLib, casTable);
  if tableExists = 0 then do;
    print "Loading table: "||casLib||"."||casTable;
	table.loadTable result=r status=rc/ caslib=casLib path=(casTable || ".sashdat") casout={caslib=casLib};
    if rc.statusCode != 0 then do;
      exit();
    end;
  end;
end func;
run;

loadTableNotExists('HRSA', 'TOMS_FILE');
dropTableIfExists('COVID19', 'cov_wea');
dropTableIfExists('COVID19', 'cov_mob');
dropTableIfExists('COVID19', 'cov_pop');
dropTableIfExists('COVID19', 'cov_wea_mob_pop');
dropTableIfExists('COVID19', 'cov_mob_cal');
dropTableIfExists('COVID19', 'cov_mob_trans');
dropTableIfExists('COVID19', 'cov_wea_del');
dropTableIfExists('COVID19', 'COVID19_ABT');
run;
quit;

data _NULL_;
   time_slept=sleep(5,1);
run;

/*cas casauto terminate;*/
